# OpenMatch

Low-latency, single-threaded matching core in C11 with a cache-friendly slab
allocator, orderbook, and durable WAL replay. Built for HFT‑style workloads,
but small and embeddable.

## Features

- **Dual slab allocator** for fixed hot fields + separate aux (cold) data.
- **Orderbook** with price ladder (Q1) and time FIFO (Q2) queues.
- **Org queue (Q3)** per product for org-level batch cancel.
- **Order ID hashmap** for O(1) cancel/lookup.
- **Write‑ahead log (WAL)** with CRC32 option, replay, and sequence recovery.
- **Matching engine** with callback hooks and WAL deal logging.
- **Perf presets** for HFT, durability, recovery, and minimal memory.

## Project Layout

```
.
├── include/openmatch/        # Public headers
│   ├── om_slab.h             # Dual slab allocator + slot layout
│   ├── orderbook.h           # Orderbook API
│   ├── om_hash.h             # Hashmap (khash/khashl)
│   ├── om_wal.h              # WAL API + replay
│   ├── om_perf.h             # Performance presets
│   ├── om_engine.h           # Matching engine API
│   └── om_wal_mock.h         # WAL mock (prints to stderr)
├── src/                      # Implementations
│   └── om_engine.c           # Matching engine
├── tests/                    # check-based unit tests
├── tools/                    # Utility binaries + awk helpers
│   ├── wal_reader.c
│   ├── wal_trace_oid.awk
│   ├── wal_match_by_maker.awk
│   └── wal_sum_qty_by_maker.awk
└── deps/                     # submodules (klib, check)
```

## Build

```bash
git clone --recursive <repo>
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### Sanitizers (default on)

```bash
cmake -DENABLE_ASAN=OFF -DENABLE_UBSAN=OFF ..
make -j$(nproc)
```

## Tests

All tests run from `build/`.

```bash
cd build
ctest --output-on-failure
```

If ASan complains about preload order, run:

```bash
cd build
LD_PRELOAD=/usr/lib/libasan.so make test
```

## Core Concepts

### Slab Allocator (`om_slab`)

Each order lives in a fixed-size slab slot (`OmSlabSlot`):

- **Hot fields** (price, volume, flags, org, order_id)
- **4 intrusive queue nodes** per slot
- **Flexible user data** for secondary hot payload
- **Aux slab** for cold data (separate allocation)

Queues:

- **Q0** internal free list
- **Q1** price ladder
- **Q2** time FIFO at price
- **Q3** org queue

### Orderbook (`orderbook`)

Per product:

- **Q1** is a sorted price ladder (best price at head)
- **Q2** is FIFO list for orders at each price level
- **Q3** is org queue per product (for batch cancel)
- **Hashmap** maps `order_id → (slot_idx, product_id)` for O(1) cancel

API highlights:

- `om_orderbook_init()` / `om_orderbook_destroy()`
- `om_orderbook_insert()`
- `om_orderbook_cancel()`
- `om_orderbook_cancel_org_product()` / `om_orderbook_cancel_org_all()`
- `om_orderbook_get_best_bid()` / `om_orderbook_get_best_ask()`

### WAL (`om_wal`)

Append-only log with replay support. Record types:

- `OM_WAL_INSERT` (variable length: fixed fields + user + aux data)
- `OM_WAL_CANCEL`
- `OM_WAL_MATCH`
- `OM_WAL_DEACTIVATE` / `OM_WAL_ACTIVATE`

Replay API:

- `om_wal_replay_init_with_config()`
- `om_wal_replay_next()` returns `-2` on CRC mismatch
- `om_orderbook_recover_from_wal()` reconstructs slab + orderbook

Custom records:

- use `om_wal_append_custom()` with types `>= OM_WAL_USER_BASE`
- register a replay handler with `om_wal_replay_set_user_handler()`

Custom record output:

- `wal_reader` prints `user[len]` for custom records
- `wal_mock` prints `type[USER] ut[<type>] len[<len>]`

Multi-file WAL:

- Set `OmWalConfig.filename_pattern` (e.g. `/tmp/openmatch_%06u.wal`) with `file_index` start
- Set `wal_max_file_size` to roll to the next file on flush
- Replay will scan sequential files in increasing index until a file is missing

### Performance Presets (`om_perf`)

Presets include `OM_PERF_DEFAULT`, `OM_PERF_HFT`, `OM_PERF_DURABLE`,
`OM_PERF_RECOVERY`, `OM_PERF_MINIMAL`. Use `om_perf_validate()` and
`om_perf_autotune()` to verify/tune.

**Approximate throughput (single‑thread, light callbacks, in‑memory):**

1. **OM_PERF_HFT**: ~2–6M matches/sec/core
2. **OM_PERF_RECOVERY**: ~1.5–4M matches/sec/core
3. **OM_PERF_DEFAULT**: ~1–3M matches/sec/core
4. **OM_PERF_MINIMAL**: ~0.5–1.5M matches/sec/core
5. **OM_PERF_DURABLE**: ~0.2–0.8M matches/sec/core

Engine can apply a preset via `OmEngineConfig.perf` or `om_engine_init_perf()`.

### Engine (`om_engine`)

Callback-driven matching core. Supports:

- `can_match` (per maker/taker; returns max match volume)
- `on_match` (per order, post‑deduction)
- `on_deal` (per trade)
- `on_booked` / `on_filled` / `on_cancel`
- `pre_booked` (decide whether remainder rests)

Order deactivation/activation:

- `om_engine_deactivate(order_id)` (remove from book, keep slot)
- `om_engine_activate(order_id)` (reattempt match as taker)

## Example (Minimal)

```c
#include "openmatch/orderbook.h"
#include "openmatch/om_wal.h"

OmSlabConfig slab_cfg = { .user_data_size = 64, .aux_data_size = 128, .total_slots = 100000 };
OmWalConfig wal_cfg = {
    .filename = "/tmp/openmatch.wal",
    .buffer_size = 1024 * 1024,
    .sync_interval_ms = 10,
    .use_direct_io = true,
    .enable_crc32 = true,
    .user_data_size = slab_cfg.user_data_size,
    .aux_data_size = slab_cfg.aux_data_size
};

OmWal wal;
om_wal_init(&wal, &wal_cfg);

OmOrderbookContext ctx;
om_orderbook_init(&ctx, &slab_cfg, &wal, 1024, 1024, 0);

OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
om_slot_set_order_id(slot, om_slab_next_order_id(&ctx.slab));
om_slot_set_price(slot, 10000);
om_slot_set_volume(slot, 100);
om_slot_set_volume_remain(slot, 100);
om_slot_set_flags(slot, OM_SIDE_BID | OM_TYPE_LIMIT);
om_slot_set_org(slot, 1);

om_orderbook_insert(&ctx, 0, slot);

om_wal_flush(&wal);
om_wal_fsync(&wal);
om_orderbook_destroy(&ctx);
om_wal_close(&wal);
```

## Roadmap / Missing Parts

1. **Snapshotting** (state dump + WAL checkpointing).
2. **Async I/O** in WAL (flag exists, not implemented).
3. **More validation** (parameter checks, error codes, and recovery guarantees):
   - bounds checks for product/org ids
   - enforce volume/price invariants (non‑zero, monotonic rules)
   - sanity checks on callbacks (e.g., can_match > 0 implies matchable)
   - WAL replay strictness options (drop/stop on corrupt record)

## Tools

### wal_reader

Reads a WAL file and prints one record per line.

```
./build/tools/wal_reader <wal_file>
```

Output format uses short bracketed fields (easy to parse):

```
seq[12] type[MATCH] len[40] m[100] t[200] p[10000] q[5] pid[0] ts[1700000000]
```

#### AWK examples

Trace a single order id across all records:

```
./build/tools/wal_reader /tmp/openmatch.wal \
  | awk -F'[][]' '{for (i=2;i<=NF;i+=2) if ($i=="oid" && $(i+1)==42) print $0}'
```

Extract all matches for a maker id (m[]):

```
./build/tools/wal_reader /tmp/openmatch.wal \
  | awk -F'[][]' '{for (i=2;i<=NF;i+=2) if ($i=="m" && $(i+1)==42) print $0}'
```

Compute total traded quantity per maker id:

```
./build/tools/wal_reader /tmp/openmatch.wal \
  | awk -F'[][]' '
    {m=""; q=""; for (i=2;i<=NF;i+=2) {if ($i=="m") m=$(i+1); if ($i=="q") q=$(i+1)}
     if (m!="" && q!="") sum[m]+=q}
    END {for (id in sum) print id, sum[id]}
  '
```

List all activates/deactivates:

```
./build/tools/wal_reader /tmp/openmatch.wal \
  | awk -F'[][]' '{for (i=2;i<=NF;i+=2) if ($i=="type" && ($(i+1)=="DEACTIVATE" || $(i+1)=="ACTIVATE")) print $0}'
```

### wal_query (SQLite extension)

Builds a loadable SQLite extension that exposes WAL records as a virtual table.

```
cmake -S . -B build
cmake --build build
```

Load in sqlite3:

```
.load ./build/tools/wal_query
CREATE VIRTUAL TABLE wal USING wal_query(
  file=/tmp/openmatch.wal,
  user_data=64,
  aux_data=128,
  crc32=1
);
SELECT seq, type_name, order_id, price, volume, product_id, timestamp_ns FROM wal;
```

Virtual table schema:

```
seq INTEGER
type INTEGER
type_name TEXT
data_len INTEGER
order_id INTEGER
price INTEGER
volume INTEGER
vol_remain INTEGER
org INTEGER
flags INTEGER
product_id INTEGER
timestamp_ns INTEGER
slot_idx INTEGER
maker_id INTEGER
taker_id INTEGER
match_price INTEGER
match_volume INTEGER
user_type INTEGER
```

Multi-file WAL (pattern + index):

```
CREATE VIRTUAL TABLE wal USING wal_query(
  pattern=/tmp/openmatch_%06u.wal,
  index=0,
  user_data=64,
  aux_data=128
);
```

### wal_mock (compile-time)

Build the library with WAL mock (prints to stderr, no file I/O):

```
cmake -S . -B build -DOM_USE_WAL_MOCK=ON
cmake --build build
```

Mock output uses the same bracketed format as `wal_reader`, for example:

```
seq[12] type[MATCH] m[100] t[200] p[10000] q[5] pid[0] ts[1700000000]
```
