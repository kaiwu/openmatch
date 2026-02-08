# om_bus — WAL Distribution Bus

Design document for `om_bus`, a third library (`libombus`) that distributes WAL
record streams across process and machine boundaries.

**Status**: Design (no implementation yet)

## 1. Overview & Goals

OpenMatch currently distributes WAL records via `OmMarketRing` — a lock-free
1P-NC ring buffer that passes `void *ptr` between threads within a single
process. This works well for co-located workers but cannot:

- Cross process boundaries (pointers are address-space-local)
- Cross machine boundaries (no network transport)
- Detect sequence gaps (no WAL sequence tracking in ring)
- Replay missed records (consumers that fall behind are stuck)

`om_bus` solves these problems with two transports:

| Transport | Scope | Latency | Use Case |
|-----------|-------|---------|----------|
| Shared memory (SHM) | Same host, cross-process | ~50-80ns publish, ~30-50ns poll | Local workers in separate processes |
| TCP | Cross-machine | ~10-50us | Remote workers, DR replicas |

### Design Principles

1. **WAL is source of truth.** The bus is a stateless transport layer. Any
   consumer can recover from a WAL replay — the bus never needs to store
   history.
2. **No routing.** Every consumer sees the full stream. Workers filter
   internally (by product, org, record type) as they do today.
3. **Backpressure over loss.** A slow consumer blocks the producer rather
   than silently dropping records (same semantics as `OmMarketRing`).
4. **WAL sequence numbers as universal cursor.** Every bus record carries
   `wal_seq` — a consumer can resume from any point by replaying the WAL
   from that sequence forward.
5. **Hot path untouched.** The engine publish path (engine -> SHM ring) adds
   a memcpy + atomic store. TCP distribution runs in a separate relay process
   that reads SHM as a consumer.

## 2. Logical Endpoint Model

Three concepts:

- **Stream** — a named, append-only log with a single producer. One stream
  per WAL instance (e.g. `"prod-engine-0"`). The producer writes records
  into a shared memory ring.
- **Endpoint** — a named consumer that reads from a stream. Each endpoint
  has its own cursor and tail position. Endpoints are registered at
  stream creation time (fixed consumer slots).
- **Cursor** — a WAL sequence number (`uint64_t`). Consumers track their
  position as a cursor. To resume after restart, replay the WAL from
  cursor forward, then attach to the live stream.

### Topology

```
                   ┌───────────────────────────────────────────────┐
                   │                  Host A                       │
                   │                                               │
                   │  ┌──────────┐    SHM ring     ┌────────────┐ │
                   │  │ OmEngine │───────────────→│ Worker P0  │ │
                   │  │ (producer)│                 │ (endpoint) │ │
                   │  └──────────┘        │        └────────────┘ │
                   │                      │        ┌────────────┐ │
                   │                      ├──────→│ Worker P1  │ │
                   │                      │        │ (endpoint) │ │
                   │                      │        └────────────┘ │
                   │                      │        ┌────────────┐ │
                   │                      └──────→│ TCP Relay  │ │
                   │                               │ (endpoint) │ │
                   │                               └─────┬──────┘ │
                   └─────────────────────────────────────┼────────┘
                                                         │ TCP
                   ┌─────────────────────────────────────┼────────┐
                   │                  Host B             │        │
                   │                               ┌─────▼──────┐ │
                   │                               │ TCP Client │ │
                   │                               └─────┬──────┘ │
                   │                      ┌──────────────┼───┐    │
                   │                      │              │   │    │
                   │                ┌─────▼────┐  ┌─────▼────┐   │
                   │                │ Worker R0│  │ Worker R1│   │
                   │                └──────────┘  └──────────┘   │
                   └──────────────────────────────────────────────┘
```

## 3. Shared Memory Transport (Tier 1 — IPC)

### 3.1 Ring Slot Format

Fixed-size slots in a memory-mapped file. Default slot size: **256 bytes**
(configurable at stream creation). Each slot contains a header followed by
inline payload data — no pointers, so the data is valid across address spaces.

```
┌─────────────────────────── 256 bytes (default) ──────────────────────────┐
│                                                                          │
│  ┌─────────────────────── 24 bytes: header ────────────────────────┐     │
│  │ _Atomic uint64_t  seq;        // slot sequence (publish fence)  │     │
│  │ uint64_t          wal_seq;    // WAL sequence number            │     │
│  │ uint8_t           wal_type;   // OmWalType (INSERT, CANCEL...) │     │
│  │ uint8_t           reserved;   // padding                        │     │
│  │ uint16_t          payload_len; // bytes of payload following    │     │
│  │ uint32_t          crc32;      // CRC32 of payload              │     │
│  └─────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌─────────────────── (slot_size - 24) bytes: payload ─────────────┐    │
│  │ Raw WAL record data (OmWalInsert, OmWalCancel, OmWalMatch...)   │    │
│  │ Copied inline — same binary layout as WAL on-disk records       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

**Slot header: `OmBusSlotHeader`** (24 bytes):

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `seq` | `_Atomic uint64_t` | 8B | Monotonic slot sequence; publish fence |
| `wal_seq` | `uint64_t` | 8B | WAL sequence from `OmWalHeader` (40-bit packed) |
| `wal_type` | `uint8_t` | 1B | `OmWalType` enum value |
| `reserved` | `uint8_t` | 1B | Alignment padding |
| `payload_len` | `uint16_t` | 2B | Payload byte count (max `slot_size - 24`) |
| `crc32` | `uint32_t` | 4B | CRC32 of payload bytes |

The `seq` field serves the same role as `OmMarketRingSlot.seq` — consumers
spin on it to detect when a slot is ready. The additional fields (`wal_seq`,
`wal_type`, `payload_len`, `crc32`) enable gap detection, filtering, and
integrity checking without parsing the payload.

**Slot sizing**: The default 256B accommodates all current WAL record types:

| WAL Type | Header | Payload | Total |
|----------|--------|---------|-------|
| INSERT (no user/aux data) | 24B | 56B (`OmWalInsert`) | 80B |
| INSERT (32B user + 64B aux) | 24B | 152B | 176B |
| CANCEL | 24B | 24B | 48B |
| MATCH | 24B | 40B | 64B |
| DEACTIVATE | 24B | 24B | 48B |
| ACTIVATE | 24B | 24B | 48B |

For workloads with large user/aux data, increase slot size at stream creation.
Records that exceed `slot_size - 24` are an error (publish returns
`OM_ERR_BUS_RECORD_TOO_LARGE`).

### 3.2 Memory Layout

The SHM file is created via `shm_open()` + `ftruncate()` + `mmap()` with a
fixed layout:

```
Offset 0                                                    Offset N
┌─────────────┬────────────────────────┬────────────────────────────┐
│ Header Page │ Consumer Tails Section │ Ring Slots                 │
│   (4 KB)    │ (max_consumers × 64B) │ (capacity × slot_size)     │
└─────────────┴────────────────────────┴────────────────────────────┘
```

**Header page** (4096 bytes, page-aligned):

```c
typedef struct OmBusShmHeader {
    uint32_t magic;             // 0x4F4D4253 ("OMBS")
    uint32_t version;           // Protocol version (1)
    uint32_t slot_size;         // Bytes per slot (default 256)
    uint32_t capacity;          // Number of slots (power of two)
    uint32_t max_consumers;     // Maximum consumer count
    uint32_t flags;             // Feature flags (CRC enable, etc.)
    _Atomic uint64_t head;      // Producer head position
    _Atomic uint64_t min_tail;  // Cached minimum consumer tail
    char stream_name[64];       // Null-terminated stream name
    uint8_t _pad[4096 - 104];   // Pad to full page
} OmBusShmHeader;
```

**Consumer tails section**: Each consumer gets a 64-byte aligned slot to
prevent false sharing (same pattern as `OmMarketRing.consumer_tails`, but
64B-aligned instead of 8B):

```c
typedef struct OmBusConsumerTail {
    _Atomic uint64_t tail;      // Consumer read position
    _Atomic uint64_t wal_seq;   // Last WAL sequence consumed
    uint8_t _pad[48];           // Pad to 64 bytes (cache line)
} OmBusConsumerTail;
```

**Ring slots section**: `capacity` slots of `slot_size` bytes each, starting
at offset `4096 + max_consumers * 64`.

### 3.3 Publish Path

The producer (engine thread) publishes a WAL record to the bus after writing
it to the WAL buffer. This is the hot path.

```
1. Compute slot index:    idx = head & mask
2. Backpressure check:    spin while (head - min_tail) >= capacity
                          (refresh min_tail from consumer tails every 32 spins)
3. Copy payload:          memcpy(slot[idx].payload, wal_record, len)
4. Fill header:           slot[idx].wal_seq = seq
                          slot[idx].wal_type = type
                          slot[idx].payload_len = len
                          slot[idx].crc32 = crc32(payload)
5. Publish fence:         atomic_store_explicit(&slot[idx].seq, head + 1,
                                               memory_order_release)
6. Advance head:          atomic_store_explicit(&header->head, head + 1,
                                               memory_order_release)
```

**Expected latency**: ~50-80ns (memcpy dominates for typical payloads).

The backpressure spin loop is identical in structure to
`om_market_ring_enqueue()` — spin on `min_tail`, periodic recomputation,
`sched_yield()` on extended contention.

### 3.4 Poll Path

Consumers poll the ring by checking the slot sequence number. Each consumer
tracks its own tail independently.

```
1. Compute slot index:    idx = my_tail & mask
2. Check ready:           if (atomic_load_explicit(&slot[idx].seq,
                              memory_order_acquire) != my_tail + 1) → empty
3. Read payload:          memcpy(out_buf, slot[idx].payload, slot[idx].payload_len)
                          — or zero-copy: return pointer into mmap region
4. Gap check:             if (slot[idx].wal_seq != expected_wal_seq) → gap detected
5. Advance tail:          atomic_store_explicit(&consumer_tails[my_idx].tail,
                              my_tail + 1, memory_order_release)
6. Update min_tail:       same refresh logic as OmMarketRing
```

**Expected latency**: ~30-50ns per record (atomic load + optional memcpy).

**Zero-copy option**: Since the mmap region is shared read-only for consumers,
they can read directly from the slot without copying. The payload remains valid
until the producer wraps around and overwrites the slot. Safe when
`capacity >> batch_size` (i.e., the consumer processes records faster than the
producer wraps).

### 3.5 Backpressure

Same model as `OmMarketRing`: the producer spins when `head - min_tail >=
capacity`. No records are dropped. If a consumer crashes or stalls, the
producer blocks until the consumer is deregistered or catches up.

For the TCP relay consumer, the relay process is responsible for draining its
SHM tail promptly. If the relay falls behind (e.g., TCP backlog), the engine
blocks — this is intentional (backpressure propagates to the producer rather
than silently losing records).

### 3.6 Gap Detection

Each bus slot carries `wal_seq` from the original WAL header. Consumers track
`expected_wal_seq` and compare on each poll:

- `slot.wal_seq == expected`: normal, increment expected
- `slot.wal_seq > expected`: gap detected — records `[expected, slot.wal_seq)`
  were not seen. Consumer initiates WAL replay for the missing range.
- `slot.wal_seq < expected`: duplicate or reorder — skip (log warning)

Gap detection is primarily useful after consumer restart. During normal
operation with backpressure, gaps should not occur.

### 3.7 Consumer Registration

Consumers are pre-assigned at stream creation time via `max_consumers` in the
config. Each consumer claims a slot index in `[0, max_consumers)`. Consumer
indices are coordinated externally (e.g., configuration file, command-line
argument).

No dynamic registration — adding a consumer requires recreating the stream.
This keeps the SHM layout fixed and avoids synchronization complexity.

## 4. TCP Transport (Tier 2 — Network)

### 4.1 Architecture

TCP distribution is handled by a **relay process** — a standalone process that:

1. Attaches to a SHM stream as a consumer endpoint
2. Accepts TCP connections from remote consumers
3. Broadcasts records to all connected clients

The relay is a separate process so the engine hot path is unaffected. The
relay reads SHM at consumer speed (~30-50ns/record) and writes TCP frames.

### 4.2 Wire Protocol

All TCP frames use a 16-byte header followed by a type-specific body:

```
┌───────────────────── 16 bytes: frame header ─────────────────────┐
│ uint32_t  magic;         // 0x4F4D4246 ("OMBF")                  │
│ uint8_t   msg_type;      // OmBusMsgType enum                    │
│ uint8_t   flags;         // Per-message flags                    │
│ uint16_t  body_len;      // Bytes following this header           │
│ uint64_t  wal_seq;       // WAL sequence (0 for control msgs)    │
└──────────────────────────────────────────────────────────────────┘
```

### 4.3 Message Types

```c
typedef enum OmBusMsgType {
    OM_BUS_MSG_DATA           = 0x01,  // Single WAL record
    OM_BUS_MSG_BATCH_DATA     = 0x02,  // Batch of WAL records
    OM_BUS_MSG_HEARTBEAT      = 0x10,  // Keepalive (relay → client)
    OM_BUS_MSG_GAP_REQUEST    = 0x20,  // Client requests missing range
    OM_BUS_MSG_GAP_RESPONSE   = 0x21,  // Relay sends replayed records
    OM_BUS_MSG_SUBSCRIBE      = 0x30,  // Client subscribe request
    OM_BUS_MSG_SUBSCRIBE_ACK  = 0x31,  // Relay acknowledge
} OmBusMsgType;
```

**DATA** (single record):

```
[frame header: 16B]
[wal_type: 1B] [reserved: 1B] [payload_len: 2B] [crc32: 4B]
[payload: payload_len bytes]
```

**BATCH_DATA** (batched records):

```
[frame header: 16B, wal_seq = first record's seq]
[record_count: 2B] [reserved: 2B] [total_payload_len: 4B]
[record 0: wal_type(1) + reserved(1) + payload_len(2) + crc32(4) + payload]
[record 1: ...]
...
```

Default batch size: **256 records** (configurable). Batching amortizes TCP
framing and syscall overhead.

**HEARTBEAT**:

```
[frame header: 16B, wal_seq = latest published seq]
(no body)
```

Sent by relay every **100ms** when no DATA frames have been sent. Clients
use heartbeat timeout (e.g., 3x interval = 300ms) to detect relay failure.

**GAP_REQUEST**:

```
[frame header: 16B]
[from_seq: 8B] [to_seq: 8B]
```

Client requests records in range `[from_seq, to_seq)`.

**GAP_RESPONSE**: Same format as BATCH_DATA, with records replayed from WAL.

**SUBSCRIBE**:

```
[frame header: 16B]
[start_seq: 8B]       // WAL seq to start from (0 = latest)
[stream_name: 64B]    // Null-terminated stream name
```

**SUBSCRIBE_ACK**:

```
[frame header: 16B]
[status: 4B]          // 0 = OK, nonzero = error code
[current_seq: 8B]     // Current head WAL sequence
```

### 4.4 Connection Lifecycle

```
Client                                  Relay
  │                                       │
  │──── TCP connect ─────────────────────→│
  │──── SUBSCRIBE(start_seq, name) ─────→│
  │←─── SUBSCRIBE_ACK(status, cur_seq) ──│
  │                                       │
  │  ┌── if start_seq < cur_seq ──┐       │
  │  │   Relay sends GAP_RESPONSE │       │
  │  │   with WAL replay records  │       │
  │  └────────────────────────────┘       │
  │                                       │
  │←─── DATA / BATCH_DATA ───────────────│  (live stream)
  │←─── HEARTBEAT ───────────────────────│  (idle periods)
  │                                       │
  │──── GAP_REQUEST(from, to) ──────────→│  (if gap detected)
  │←─── GAP_RESPONSE(records) ──────────│
  │                                       │
  │──── TCP close ───────────────────────→│
```

### 4.5 Batching

The relay accumulates records from SHM and sends them as BATCH_DATA:

- Flush when batch reaches **256 records** (configurable)
- Flush when **1ms** has elapsed since first record in batch (latency bound)
- Flush immediately on `wal_type == OM_WAL_CHECKPOINT`

### 4.6 Relay Configuration

```c
typedef struct OmBusTcpRelayConfig {
    const char *stream_name;       // SHM stream to consume
    const char *bind_addr;         // Listen address (e.g., "0.0.0.0")
    uint16_t    bind_port;         // Listen port
    uint32_t    max_clients;       // Maximum concurrent clients
    uint32_t    batch_size;        // Records per batch (default 256)
    uint32_t    heartbeat_ms;      // Heartbeat interval (default 100)
    uint32_t    flush_timeout_us;  // Batch flush timeout (default 1000)
    const OmWalConfig *wal_config; // WAL config for replay (gap fill)
} OmBusTcpRelayConfig;
```

## 5. API Design

### 5.1 Error Codes

Range **-800 to -899** (following existing convention in `om_error.h`):

```c
/* Bus errors (-800 to -899) */
OM_ERR_BUS_INIT             = -800,  // Bus initialization failed
OM_ERR_BUS_SHM_CREATE       = -801,  // shm_open/ftruncate failed
OM_ERR_BUS_SHM_MAP          = -802,  // mmap failed
OM_ERR_BUS_SHM_OPEN         = -803,  // Consumer shm_open failed
OM_ERR_BUS_NOT_POW2         = -804,  // Capacity not power of two
OM_ERR_BUS_CONSUMER_ID      = -805,  // Invalid consumer index
OM_ERR_BUS_RECORD_TOO_LARGE = -806,  // Payload exceeds slot_size - 24
OM_ERR_BUS_MAGIC_MISMATCH   = -807,  // SHM header magic mismatch
OM_ERR_BUS_VERSION_MISMATCH = -808,  // SHM header version mismatch
OM_ERR_BUS_CRC_MISMATCH     = -809,  // Payload CRC32 mismatch
OM_ERR_BUS_GAP_DETECTED     = -810,  // WAL sequence gap detected
OM_ERR_BUS_TCP_BIND         = -811,  // TCP bind/listen failed
OM_ERR_BUS_TCP_CONNECT      = -812,  // TCP connect failed
OM_ERR_BUS_TCP_SEND         = -813,  // TCP send failed
OM_ERR_BUS_TCP_RECV         = -814,  // TCP recv failed
OM_ERR_BUS_SUBSCRIBE_FAIL   = -815,  // Subscribe rejected by relay
```

### 5.2 Core Structures

```c
/* Record delivered to consumers */
typedef struct OmBusRecord {
    uint64_t wal_seq;           // WAL sequence number
    uint8_t  wal_type;          // OmWalType enum value
    uint16_t payload_len;       // Payload byte count
    const void *payload;        // Pointer to payload data
                                // (SHM: points into mmap region or copy buffer)
                                // (TCP: points into recv buffer)
} OmBusRecord;
```

### 5.3 Producer API (SHM Stream)

```c
typedef struct OmBusStreamConfig {
    const char *stream_name;    // SHM object name (e.g., "/om-bus-engine-0")
    uint32_t    capacity;       // Ring capacity, power of two (default 4096)
    uint32_t    slot_size;      // Bytes per slot (default 256)
    uint32_t    max_consumers;  // Maximum consumer count (default 8)
    uint32_t    flags;          // Feature flags (OM_BUS_FLAG_CRC, etc.)
} OmBusStreamConfig;

typedef struct OmBusStream OmBusStream;  // opaque

/**
 * Create a new SHM stream (producer side).
 * Creates the shared memory file and initializes the ring.
 * @return 0 on success, negative on error
 */
int om_bus_stream_create(OmBusStream **out, const OmBusStreamConfig *config);

/**
 * Publish a WAL record to the stream.
 * Copies payload into the next ring slot. Blocks (spins) if ring is full.
 * @param wal_seq  WAL sequence number from om_wal_pack_header()
 * @param wal_type OmWalType enum value
 * @param payload  Raw WAL record data
 * @param len      Payload byte count
 * @return 0 on success, OM_ERR_BUS_RECORD_TOO_LARGE if len > slot_size - 24
 */
int om_bus_stream_publish(OmBusStream *stream, uint64_t wal_seq,
                          uint8_t wal_type, const void *payload, uint16_t len);

/**
 * Destroy stream and unlink SHM object.
 */
void om_bus_stream_destroy(OmBusStream *stream);
```

### 5.4 Consumer API (SHM Endpoint)

```c
typedef struct OmBusEndpointConfig {
    const char *stream_name;    // SHM object name to attach to
    uint32_t    consumer_index; // Pre-assigned consumer index
    bool        zero_copy;      // If true, OmBusRecord.payload points into mmap
} OmBusEndpointConfig;

typedef struct OmBusEndpoint OmBusEndpoint;  // opaque

/**
 * Open an endpoint to an existing SHM stream (consumer side).
 * Maps the shared memory file read-only (+ consumer tail read-write).
 * @return 0 on success, negative on error
 */
int om_bus_endpoint_open(OmBusEndpoint **out, const OmBusEndpointConfig *config);

/**
 * Poll for the next record. Non-blocking.
 * @param rec  Output record (payload pointer valid until next poll or close)
 * @return 1 if record available, 0 if empty, negative on error
 *         OM_ERR_BUS_GAP_DETECTED if wal_seq gap found (rec still populated)
 *         OM_ERR_BUS_CRC_MISMATCH if CRC check fails
 */
int om_bus_endpoint_poll(OmBusEndpoint *ep, OmBusRecord *rec);

/**
 * Poll up to max_count records in a batch. Non-blocking.
 * @param recs       Output record array
 * @param max_count  Maximum records to return
 * @return Number of records read (0 = empty), negative on error
 */
int om_bus_endpoint_poll_batch(OmBusEndpoint *ep, OmBusRecord *recs,
                               size_t max_count);

/**
 * Get current WAL sequence position for this endpoint.
 */
uint64_t om_bus_endpoint_wal_seq(const OmBusEndpoint *ep);

/**
 * Close endpoint and unmap SHM.
 */
void om_bus_endpoint_close(OmBusEndpoint *ep);
```

### 5.5 WAL Replay API

Consumers that detect a gap can replay the WAL to fill in missing records.
This wraps `OmWalReplay` with bus-compatible output:

```c
typedef struct OmBusReplayConfig {
    const char *wal_filename;       // WAL file path or pattern
    const OmWalConfig *wal_config;  // WAL config (for data sizes, CRC)
    uint64_t from_seq;              // Start WAL sequence (inclusive)
    uint64_t to_seq;                // End WAL sequence (exclusive, 0 = EOF)
} OmBusReplayConfig;

typedef struct OmBusReplay OmBusReplay;  // opaque

/**
 * Initialize replay iterator for a WAL sequence range.
 * @return 0 on success, negative on error
 */
int om_bus_replay_init(OmBusReplay **out, const OmBusReplayConfig *config);

/**
 * Read next replayed record.
 * @return 1 if record available, 0 if end of range/EOF, negative on error
 */
int om_bus_replay_next(OmBusReplay *replay, OmBusRecord *rec);

/**
 * Close replay iterator.
 */
void om_bus_replay_close(OmBusReplay *replay);
```

### 5.6 TCP Client API

For remote consumers connecting to a TCP relay:

```c
typedef struct OmBusTcpClientConfig {
    const char *relay_host;        // Relay hostname or IP
    uint16_t    relay_port;        // Relay port
    const char *stream_name;       // Stream to subscribe to
    uint64_t    start_seq;         // WAL seq to start from (0 = latest)
    uint32_t    recv_buffer_size;  // TCP receive buffer (default 1MB)
} OmBusTcpClientConfig;

typedef struct OmBusTcpClient OmBusTcpClient;  // opaque

/**
 * Connect to a TCP relay and subscribe to a stream.
 * Blocks until SUBSCRIBE_ACK is received.
 * @return 0 on success, negative on error
 */
int om_bus_tcp_connect(OmBusTcpClient **out, const OmBusTcpClientConfig *config);

/**
 * Poll for next record from TCP stream. Non-blocking.
 * @return 1 if record available, 0 if no data, negative on error
 */
int om_bus_tcp_poll(OmBusTcpClient *client, OmBusRecord *rec);

/**
 * Poll batch from TCP stream.
 * @return Number of records read (0 = empty), negative on error
 */
int om_bus_tcp_poll_batch(OmBusTcpClient *client, OmBusRecord *recs,
                          size_t max_count);

/**
 * Request gap fill from relay.
 * @return 0 on success (records arrive via poll), negative on error
 */
int om_bus_tcp_request_gap(OmBusTcpClient *client,
                           uint64_t from_seq, uint64_t to_seq);

/**
 * Disconnect and free resources.
 */
void om_bus_tcp_close(OmBusTcpClient *client);
```

## 6. Resilience Model

### 6.1 Sequence Gap Detection

Every poll checks `slot.wal_seq` against `expected_wal_seq`:

| Scenario | Detection | Recovery |
|----------|-----------|----------|
| Consumer restart | First poll after attach finds wal_seq > saved cursor | WAL replay from saved cursor to current head |
| Producer restart | New stream starts at wal_seq 1 (or WAL recovery seq) | Consumer detects sequence reset, full WAL replay |
| SHM corrupted | Magic/version mismatch on attach | Consumer refuses to attach, error returned |

### 6.2 Consumer Crash Recovery

1. Consumer process crashes mid-poll.
2. Consumer tail remains at the pre-crash position in SHM.
3. Producer may block if this consumer was the min_tail. **Mitigation**:
   a monitoring process detects stale tails (no progress for N seconds) and
   can reset them, or the consumer restarts and resumes from its last
   persisted cursor.
4. On restart, consumer:
   a. Reads its last persisted `wal_seq` (e.g., from a local checkpoint file)
   b. Replays WAL from that sequence forward via `om_bus_replay_init()`
   c. Attaches to SHM stream, catches up from current ring position

### 6.3 Producer Failover

The producer (engine) is single-writer. If it crashes:

1. WAL on disk is the recovery source.
2. New engine instance recovers orderbook state from WAL
   (`om_orderbook_recover_from_wal()`).
3. New engine creates a fresh SHM stream (or reuses the same name).
4. Consumers detect the new stream (magic/version or stale head) and
   reconnect, replaying WAL from their last checkpoint.

### 6.4 Slow Consumer Handling

- **SHM**: Backpressure — producer blocks. If a consumer is permanently slow,
  it must be deregistered (increase `max_consumers` and reassign slots, or
  reduce the consumer's workload).
- **TCP**: The relay process buffers in a per-client send queue. If the queue
  exceeds a high-water mark, the relay disconnects the slow client. The client
  can reconnect and replay from its last known `wal_seq`.

## 7. Performance

### 7.1 Targets

| Metric | SHM | TCP |
|--------|-----|-----|
| Publish latency | ~50-80ns | N/A (relay is async) |
| Poll latency | ~30-50ns | ~10-50us (network RTT) |
| Throughput | >5M records/sec | ~500K-1M records/sec (batched) |
| Memory per stream | 4KB + consumers×64B + capacity×slot_size | +send buffers |

### 7.2 Memory Budget

Default configuration: 4096 slots × 256B = 1MB ring + 4KB header + 512B tails
(8 consumers × 64B) = **~1.05 MB per stream**.

Aggressive configuration: 16384 slots × 512B = 8MB ring + 4KB + 2KB =
**~8 MB per stream**.

### 7.3 Comparison with OmMarketRing

| | OmMarketRing | om_bus SHM |
|---|---|---|
| Scope | Intra-process (threads) | Inter-process (mmap) |
| Payload | `void *ptr` (8B) | Inline data (up to slot_size - 24B) |
| Gap detection | None | WAL sequence tracking |
| Replay | None | WAL-based |
| Consumer crash | Producer blocks forever | Stale tail detection + reset |
| Publish cost | ~20-30ns (store ptr) | ~50-80ns (memcpy + atomics) |
| Poll cost | ~15-25ns (load ptr) | ~30-50ns (read mmap + optional copy) |
| Memory | 16B × capacity | slot_size × capacity |

The bus adds ~30-50ns overhead vs the in-process ring. This is acceptable
because the bus path is for cross-process distribution — the hot-path
intra-process ring (`OmMarketRing`) remains available for co-located workers.

## 8. Integration

### 8.1 Engine Publish Hook

The engine publishes to the bus after each WAL write. This can be implemented
as a post-write hook in `om_wal_insert`/`om_wal_cancel`/`om_wal_match` or
as an explicit call in the engine after each WAL operation:

```c
// In engine match/insert/cancel path:
uint64_t seq = om_wal_insert(wal, slot, product_id);
if (bus_stream) {
    om_bus_stream_publish(bus_stream, seq, OM_WAL_INSERT,
                          &insert_record, sizeof(insert_record) + data_sizes);
}
```

### 8.2 Worker Migration

Workers currently consume from `OmMarketRing` via `om_market_ring_dequeue()`.
Migration to bus consumers:

```c
// Before (intra-process):
void *ptr;
int rc = om_market_ring_dequeue(ring, consumer_idx, &ptr);

// After (inter-process via bus):
OmBusRecord rec;
int rc = om_bus_endpoint_poll(endpoint, &rec);
// rec.payload contains the same WAL record data
```

The worker's internal processing logic (`om_market_ingest()`) remains
unchanged — it receives the same WAL record types regardless of transport.

### 8.3 Build System

`om_bus` is built as a third library alongside `libopenmatch` and
`libopenmarket`:

```
libombus.so / libombus.a
```

Dependencies:
- `libopenmatch` (for `OmWalType`, `OmWalConfig`, WAL replay)
- `librt` (for `shm_open`, POSIX shared memory)
- `libpthread` (for atomics, relay threading)

### 8.4 File Layout

```
include/
  ombus/
    om_bus.h            // Public API (stream, endpoint, record)
    om_bus_tcp.h        // TCP client + relay API
    om_bus_replay.h     // Replay API
    om_bus_error.h      // Bus-specific error codes (or extend om_error.h)
src/
  om_bus_shm.c          // SHM stream + endpoint implementation
  om_bus_tcp_relay.c    // TCP relay process
  om_bus_tcp_client.c   // TCP client implementation
  om_bus_replay.c       // WAL replay adapter
```

### 8.5 Migration Path

| Phase | Change | Risk |
|-------|--------|------|
| **1. Library skeleton** | Create `libombus`, headers, error codes, build integration | None (no runtime change) |
| **2. SHM transport** | Implement `om_bus_stream_*` and `om_bus_endpoint_*` | Low (new code, tested independently) |
| **3. Engine hook** | Add `om_bus_stream_publish()` calls after WAL writes | Low (additive, OmMarketRing unchanged) |
| **4. Worker dual-read** | Workers can read from either OmMarketRing or bus endpoint | Medium (workers need config switch) |
| **5. TCP relay** | Implement relay process and TCP client | Low (separate process) |

Phase 1-3 can ship without changing any existing worker behavior.
`OmMarketRing` continues to work for intra-process workers. The bus is
opt-in for new cross-process deployments.
