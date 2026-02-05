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

## Summary

The slab + intrusive queue design mirrors OpenMatch's architecture:
- Fixed-size slots in a contiguous slab (cache-line aligned)
- Intrusive queues with uint32_t indices (not pointers)
- Q0 for free list, Q1 for sorted price ladder
- Hash map for O(1) price → slot lookup
- Each worker owns its slab (no cross-worker sharing)
- Bounded memory, no memmove, O(N) publish
