# Market Data Aggregation (OpenMarket)

This document describes how OpenMarket consumes WAL records and builds
**public** and **private** market data per product/price/side.

## Definitions

- **Public ladder**: total remaining quantity at each price level for a product.
- **Private ladder**: dealable remaining quantity per org at each price level.
- **Top-N**: the top `N` price levels published per side (bid/ask).
- **Dealable callback**: `uint64_t dealable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx)`
  returns the maximum dealable quantity for an org at insert time (0 = not dealable).

## Data Ownership & Sharding

- **Private workers** are **sharded by org**.
- **Public workers** are **sharded by product** (via `product_to_public_worker`).
- Private and public workers can consume the **same** WAL stream or different streams.
- Each worker maintains its own slab, ladders, and order maps (no cross-worker writes).

## Memory Architecture: Slab + Intrusive Queue

OpenMarket uses the same architectural pattern as OpenMatch:
- **Slab allocator** for fixed-size price level slots
- **Intrusive queues** linking slots together (no external node allocation)
- **uint32_t indices** instead of pointers (fits more in cache line)
- **Hash map** for O(1) price → slot lookup

### Price Level Slot (64 bytes = 1 cache line)

```c
#define OM_MARKET_SLOT_NULL UINT32_MAX

typedef struct OmMarketLevelSlot {
    // Q0: free list links (8 bytes)
    uint32_t q0_next;           // next free slot
    uint32_t q0_prev;           // prev free slot

    // Q1: price ladder links (8 bytes)
    uint32_t q1_next;           // next slot (worse price)
    uint32_t q1_prev;           // prev slot (better price)

    // Data (16 bytes)
    uint64_t price;
    uint64_t qty;

    // Metadata (8 bytes)
    uint32_t ladder_idx;        // which ladder owns this slot
    uint16_t side;              // OM_SIDE_BID or OM_SIDE_ASK
    uint16_t flags;             // reserved

    // Padding to 64 bytes (24 bytes)
    uint8_t reserved[24];
} OmMarketLevelSlot;            // 64 bytes exactly
```

### Queue Definitions

| Queue | Purpose | Links Used | When Active |
|-------|---------|------------|-------------|
| **Q0** | Free list (slab internal) | q0_next, q0_prev | Slot is free |
| **Q1** | Price ladder (sorted) | q1_next, q1_prev | Slot is active |

A slot is either in Q0 (free) or Q1 (active), never both. The links could be
unioned, but keeping them separate allows future expansion (e.g., additional
queues for LRU eviction or time-ordered tracking).

### Slab Structure

```c
typedef struct OmMarketLevelSlab {
    OmMarketLevelSlot *slots;   // contiguous array, 64-byte aligned
    uint32_t capacity;          // total slots
    uint32_t q0_head;           // free list head
    uint32_t q0_tail;           // free list tail
    uint32_t free_count;        // slots available
} OmMarketLevelSlab;
```

### Ladder Structure (Q1 Queue Heads)

```c
KHASH_MAP_INIT_INT64(om_level_map, uint32_t)  // price → slot_idx

typedef struct OmMarketLadder {
    // Q1 heads/tails for bid side
    uint32_t bid_head;          // best bid (highest price)
    uint32_t bid_tail;          // worst bid (lowest price)
    uint32_t bid_count;         // active bid levels

    // Q1 heads/tails for ask side
    uint32_t ask_head;          // best ask (lowest price)
    uint32_t ask_tail;          // worst ask (highest price)
    uint32_t ask_count;         // active ask levels

    // O(1) price lookup
    khash_t(om_level_map) *price_to_slot;
} OmMarketLadder;
```

## Worker Memory Layout

**Each worker owns its own slab** - no sharing between workers.

### Private Worker

```
┌─────────────────────────────────────────────────────────────────┐
│ OmMarketWorker                                                  │
├─────────────────────────────────────────────────────────────────┤
│ slab: OmMarketLevelSlab                                         │
│   └─ slots[0..capacity-1]  (64-byte aligned, contiguous)        │
│                                                                 │
│ ladders[0]: (org0, prod0)                                       │
│   ├─ bid: head ──→ slot[5] ──→ slot[12] ──→ slot[3] ──→ NULL   │
│   │        (100)      (95)        (90)                          │
│   ├─ ask: head ──→ slot[8] ──→ slot[2] ──→ NULL                │
│   │        (101)      (105)                                     │
│   └─ price_to_slot: {100→5, 95→12, 90→3, 101→8, 105→2}         │
│                                                                 │
│ ladders[1]: (org0, prod1)                                       │
│   └─ ...                                                        │
│                                                                 │
│ orders[org_idx]: khash order_id → OmMarketOrderState            │
│ ladder_dirty[]: 64-byte aligned flags                           │
│ ladder_deltas[]: delta tracking hash maps                       │
└─────────────────────────────────────────────────────────────────┘
```

### Public Worker

```
┌─────────────────────────────────────────────────────────────────┐
│ OmMarketPublicWorker                                            │
├─────────────────────────────────────────────────────────────────┤
│ slab: OmMarketLevelSlab                                         │
│   └─ slots[0..capacity-1]  (64-byte aligned, contiguous)        │
│                                                                 │
│ ladders[prod0]:                                                 │
│   ├─ bid: head ──→ slot[1] ──→ slot[7] ──→ ...                 │
│   ├─ ask: head ──→ slot[4] ──→ slot[9] ──→ ...                 │
│   └─ price_to_slot: {...}                                       │
│                                                                 │
│ ladders[prod1]:                                                 │
│   └─ ...                                                        │
│                                                                 │
│ orders: khash order_id → OmMarketOrderState                     │
│ dirty[]: 64-byte aligned flags                                  │
│ deltas[]: delta tracking hash maps                              │
└─────────────────────────────────────────────────────────────────┘
```

## Operations

### Slab Operations

| Operation | Steps | Cost |
|-----------|-------|------|
| **Alloc** | Pop from Q0 head | O(1) |
| **Free** | Push to Q0 head | O(1) |
| **Grow** | Realloc slots, link new slots to Q0 tail | O(new_capacity) |

**Slab Growth**: When a slot allocation fails (slab full), the slab doubles in capacity
via `realloc`. Since we use uint32_t indices (not pointers), all existing indices remain
valid after growth. New slots are initialized and linked to the Q0 free list tail.
This is a rare event - initial sizing accounts for expected load - but ensures we never
lose WAL records due to capacity limits.

### Ladder Operations

| Operation | Steps | Cost |
|-----------|-------|------|
| **Add qty at price** | 1. Hash lookup price→slot<br>2. If found: `slot.qty += qty`<br>3. If not: alloc slot, find position in Q1, link | O(1) lookup, O(L) insert position |
| **Sub qty at price** | 1. Hash lookup price→slot<br>2. `slot.qty -= qty`<br>3. If qty==0: unlink from Q1, free to slab, remove from hash | O(1) |
| **Get qty at price** | Hash lookup price→slot, return qty | O(1) |
| **Publish top-N** | Walk Q1 from head, copy N entries | O(N), cache-friendly |

### Finding Insert Position (Q1)

For bids (descending by price): walk from head until `slot.price < new_price`.
For asks (ascending by price): walk from head until `slot.price > new_price`.

Optimization: If new price is likely near best (common case), start from head.
If new price is likely worse than current worst, start from tail.

## Record Flow

1. The matcher writes a WAL record to disk.
2. The record pointer is enqueued to the 1P-NC ring buffer.
3. Workers dequeue in batches and aggregate for their orgs/products.

## Per-Record Aggregation Behavior

### INSERT (OmWalInsert)

**Public (product-level)**

- For the public worker assigned to the product:
  - Hash lookup price in ladder
  - If found: add `vol_remain` to existing slot
  - If not found: alloc slot from slab, insert into Q1 at correct position

**Private (org-level)**

- For each subscribed org in the **private** worker for this product:
  - Call `dealable(rec, viewer_org)` -> `dq`
  - If `dq > 0`: add `min(vol_remain, dq)` to the org's ladder (same as public)

The order is stored in per-org order maps and in the public order map so later
records can resolve price/product/remaining without rescanning the book.

### CANCEL / DEACTIVATE (OmWalCancel / OmWalDeactivate)

**Public**

- Lookup `order_id` in the public order map.
- If present and active, subtract remaining quantity from the ladder.
- If qty reaches 0: unlink slot from Q1, free to slab, remove from hash.

**Private**

- Each worker checks **all** its per-org order maps.
- For each org where the order exists, subtract remaining from that org's ladder.

### ACTIVATE (OmWalActivate)

**Public**

- Lookup in public order map; if inactive and remaining > 0, re-add remaining
  to ladder (alloc slot if price level was removed).

**Private**

- For each per-org order map that contains the order, re-add remaining.

### MATCH (OmWalMatch)

**Public**

- Lookup maker order in public order map, subtract matched quantity from ladder.

**Private**

- For each per-org order map that contains the maker order, subtract matched quantity.

## Top-N Publishing

- **All price levels** are tracked in the ladder (Q1 linked list).
- Top-N filtering happens **at publish time** by walking Q1 from head.
- Since Q1 is sorted (best first), the first N slots are the top-N.

```c
// Publish top-N bids
uint32_t slot_idx = ladder->bid_head;
for (int i = 0; i < top_levels && slot_idx != OM_MARKET_SLOT_NULL; i++) {
    OmMarketLevelSlot *slot = &slab->slots[slot_idx];
    out[i].price = slot->price;
    out[i].qty = slot->qty;
    slot_idx = slot->q1_next;
}
```

## Publish Cadence

- Market snapshots are published at a fixed interval (e.g., 1s).
- If no WAL records affect a ladder, the **same snapshot** is still published.
- Dirty flags are used to skip re-serialization work when unchanged.

**Concrete publish loop (public):**

1. For each product assigned to the public worker, check `om_market_public_is_dirty()`.
2. If dirty, publish **delta** or **full** (see below) and then clear.
3. If not dirty, publish cached data (or skip) based on your cadence requirements.

**Concrete publish loop (private):**

1. For each subscribed `(org_id, product_id)` pair in the worker, check
   `om_market_worker_is_dirty()`.
2. If dirty, publish **delta** or **full** (see below) and then clear.
3. If not dirty, publish cached data (or skip) based on your cadence requirements.

## Delta vs Full Snapshot Publishing

- **Delta publishing:**
  - Only changed price levels are published each tick.
  - Backed by per-ladder delta maps (price -> delta).
  - Lowest bandwidth when changes are sparse.

- **Full snapshot publishing:**
  - Publishes top-N price levels every tick.
  - Walk Q1 from head, copy first N slots.
  - **Cache optimal**: sequential slot access if slots are allocated contiguously.

## Performance Characteristics

### Slab + Intrusive Queue Performance

| Operation | Cost | Notes |
|-----------|------|-------|
| Alloc/Free slot | O(1) | Q0 push/pop |
| Add qty (existing price) | O(1) | Hash lookup + increment |
| Add qty (new price) | O(1) + O(L) | Hash miss + Q1 insert position |
| Sub qty | O(1) | Hash lookup + decrement (+ free if zero) |
| Publish top-N | O(N) | Walk Q1, sequential memory access |
| Get qty at price | O(1) | Hash lookup |

Where L = number of price levels in the ladder.

### Comparison with Previous Designs

| Design | Insert | Remove | Publish | Memory |
|--------|--------|--------|---------|--------|
| Hash map only | O(1) | O(1) | O(P log P) sort | Unbounded |
| Dynamic sorted array | O(L) memmove | O(L) memmove | O(N) copy | Unbounded |
| **Slab + intrusive queue** | O(1) + O(L) link | O(1) | O(N) walk | **Bounded** |

Key advantages of slab + intrusive queue:
1. **Bounded memory**: Preallocated slab, no dynamic allocation during operation.
2. **No memmove**: Insert/remove just update a few indices.
3. **Cache-friendly publish**: Walk contiguous slots (if allocated sequentially).
4. **O(1) price lookup**: Hash map for direct access.

### Insert Position Optimization

Finding insert position is O(L) worst case, but can be optimized:
- **Best price heuristic**: Most inserts are near the best price; start from head.
- **Worst price check**: If new price is worse than tail, insert at tail (O(1)).
- **Skip list**: For very deep ladders (L > 100), add skip pointers.

## Slab Sizing

Each worker's slab should be sized for:

```
capacity = num_ladders * expected_levels_per_side * 2 (bid + ask) * safety_factor
```

Example:
- Private worker with 1000 subscriptions
- Expected 50 price levels per side
- Safety factor 1.5

```
capacity = 1000 * 50 * 2 * 1.5 = 150,000 slots
memory = 150,000 * 64 bytes = 9.6 MB per private worker
```

## False Sharing Prevention

- Each worker has its **own slab** (no sharing).
- Dirty flags are 64-byte aligned.
- No shared counters in the hot path.

## Optimization Suggestions

1. **Size slab appropriately**: Use `expected_price_levels * num_ladders * 2 * 1.5`.
2. **Make dealable fast**: simple bit checks, precomputed org/product rules, no I/O.
3. **Batch WAL processing**: process 256-1024 records per batch.
4. **Preallocate hash maps**: size order maps and price_to_slot maps to avoid rehash.
5. **Right-size workers**: balance load across workers to avoid hotspots.
6. **Prefer full snapshot over delta** when most levels change each tick.
7. **Insert position hint**: track whether new orders tend to be at best or spread out.

## Capacity Planning

### Reference Scenario

| Parameter | Value |
|-----------|-------|
| Orgs (O) | 5,000 |
| Products per org (P) | 10,000 |
| Total subscriptions (S = O × P) | 50,000,000 |
| Price levels per side (L) | 20 |
| WAL throughput (R) | 1,000,000/s |
| Publish window | 1.0s |
| Aggregation budget (T) | 0.5s (remaining 0.5s for serialization) |

### Per-Record Cost Model

**Public worker** — no fan-out, 1 product operation per WAL record:

| Operation | Cost |
|-----------|------|
| `product_has_subs` check | ~1ns |
| `kh_put(order_map)` | ~35ns |
| `kh_get(level_map, price)` (hit) | ~25ns |
| `slot.qty += qty` | ~5ns |
| Delta map update | ~25ns |
| **Total per record** | **~90ns** |

**Private worker** — fans out to O/W orgs per record (W = worker count):

| Operation | Cost |
|-----------|------|
| `find_ladder()` (array indexed) | ~10ns |
| `dealable()` callback | ~5ns |
| `kh_put(order_map)` | ~35ns |
| `kh_get(level_map, price)` (hit) | ~25ns |
| `slot.qty += qty` | ~5ns |
| Delta map update | ~25ns |
| Mark dirty | ~5ns |
| **Total per org** | **~110ns** |

With L2/L3 cache pressure from large working sets: **150-200ns per org realistic**.

### Public Worker Sizing

```
records_per_window = R × T = 1,000,000 × 0.5 = 500,000
time_needed = 500,000 × 90ns = 45ms
```

**1-2 public workers** is sufficient.

Memory per public worker:
```
slab = P × L × 2 sides × 64 bytes = 10,000 × 20 × 2 × 64 = 25.6 MB
hash + orders overhead ≈ 2× slab ≈ 50 MB
total ≈ 75 MB per public worker
```

### Private Worker Sizing (Fan-out at Insert)

Each private worker reads ALL WAL records and fans out to its assigned orgs.

Time budget per record: `T / (R × T) = 500ns`

```
constraint: (O / W) × cost_per_org ≤ 500ns

Optimistic  (110ns/org):  W ≥ 5,000 × 110 / 500 = 1,100
Realistic   (150ns/org):  W ≥ 5,000 × 150 / 500 = 1,500
Pessimistic (200ns/org):  W ≥ 5,000 × 200 / 500 = 2,000
```

Memory (constant regardless of W, total across all workers):
```
slab:   S × L × 2 × 64 bytes  = 50M × 40 × 64      = 128 GB
hashes: ~2× slab overhead                             =  64 GB
orders: S × avg_active_orders × 40 bytes              ≈  50 GB
────────────────────────────────────────────────────────────────
total private                                         ≈ 250 GB
```

### Scaling Limit of Fan-out at Insert

The fundamental cost is:

```
total_org_operations = R × (O / W) × W = R × O = 5 billion/s
```

This is **independent of W** — sharding reduces per-worker work but total work is
fixed. At 5,000 orgs the fan-out-at-insert design requires **~1,500 workers and
~250 GB RAM** for private aggregation alone. This does not scale.

### Alternative: Compute-on-Publish

Instead of fanning out per-org counters at WAL ingestion time:

1. **WAL processing** — same as public worker (no per-org fan-out):
   - Update public ladder + record order state per org
   - Per record: ~200ns (public ladder update + order hash put)
   - **2-3 WAL workers** handle 1M/s

2. **At publish time**, compute private view on demand:
   ```
   for each (org, product) subscription:
       walk public top-N (N levels)
       for each level: private_qty = public_qty - self_qty_at_price
   ```
   - Cost: N × ~10ns = 200ns per subscription
   - Total: 50M subs × 200ns = 10s
   - With Wp publish workers: 10s / Wp
   - **20 publish workers** to finish in 0.5s

3. **Memory**: only per-org order state, no per-org slab/ladders
   - Public slab: ~25 MB per public worker
   - Per-org order sets: 5,000 orgs × active_orders × ~40 bytes ≈ few GB
   - **Total ≈ 30 GB**

### Comparison

| | Fan-out at Insert | Compute-on-Publish |
|---|---|---|
| WAL workers | 1,500-2,000 | 2-3 |
| Publish workers | 0 (pre-computed) | 20 |
| **Total workers** | **~1,500** | **~25** |
| **Memory** | **~250 GB** | **~30 GB** |
| Private read latency | O(N) walk, instant | O(N) compute per sub |
| Complexity | Per-org slab + ladders | Per-org order set only |
| Trade-off | Pre-computed, fast read | Computed on demand |

**Recommendation**: For O > ~100 orgs, compute-on-publish is strongly preferred.
The fan-out design works well for small org counts (10-50) where the per-record
fan-out cost is bounded.

### Quick Sizing Formula

For fan-out-at-insert private workers:
```
W_private ≥ O × cost_per_org / time_budget_per_record
         = O × 150ns / (T / (R × T))
         = O × 150ns × R

Example: 50 orgs, 1M/s WAL, 0.5s budget
  W ≥ 50 × 150 / 500 = 15 workers
```

For compute-on-publish:
```
W_wal     = R × cost_per_record / T = 1M × 200ns / 0.5 ≈ 1 worker
W_publish = S × cost_per_sub / T    = S × 200ns / T

Example: 50M subs, 0.5s budget
  W_publish = 50M × 200ns / 0.5 = 20 workers
```

## Summary

The slab + intrusive queue design mirrors OpenMatch's architecture:
- Fixed-size slots in a contiguous slab (cache-line aligned)
- Intrusive queues with uint32_t indices (not pointers)
- Q0 for free list, Q1 for sorted price ladder
- Hash map for O(1) price → slot lookup
- Each worker owns its slab (no cross-worker sharing)
- Bounded memory, no memmove, O(N) publish
- Slab growth via realloc when capacity exceeded (indices survive)

For large-scale deployments (>100 orgs), consider compute-on-publish for private
ladders to avoid O(R × O) fan-out cost.
