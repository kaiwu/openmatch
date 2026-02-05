# Market Data Aggregation (OpenMarket)

This document describes how OpenMarket consumes WAL records and builds
**public** and **private** market data per product/price/side.

## Definitions

- **Public ladder**: total remaining quantity at each price level for a product.
- **Private ladder**: dealable remaining quantity per org at each price level.
- **Top-N**: the top `N` price levels published per side (bid/ask). See "Dynamic Ladder" below.
- **Dealable callback**: `uint64_t dealable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx)`
  returns the maximum dealable quantity for an org at insert time (0 = not dealable).

## Data Ownership & Sharding

- **Private workers** are **sharded by org**.
- **Public workers** are **sharded by product** (via `product_to_public_worker`).
- Private and public workers can consume the **same** WAL stream or different streams.
- Each worker maintains its own ladders and order maps (no cross-worker writes).

## Memory Layout

### Price Level Structure (16 bytes)

```c
typedef struct OmMarketLevel {
    uint64_t price;   // 8 bytes
    uint64_t qty;     // 8 bytes
} OmMarketLevel;

typedef struct OmMarketLadder {
    OmMarketLevel *bid_levels;  // dynamic sorted array for bids
    OmMarketLevel *ask_levels;  // dynamic sorted array for asks
    uint32_t bid_count;         // actual populated levels
    uint32_t ask_count;
    uint32_t bid_capacity;      // allocated capacity
    uint32_t ask_capacity;
} OmMarketLadder;
```

### Dynamic Ladder Design

Each ladder maintains a **dynamic sorted array** that tracks **ALL price levels** (not just top-N):

- Arrays are preallocated with `expected_price_levels` capacity
- Arrays grow dynamically (2x growth factor) when capacity is exceeded
- Top-N filtering happens **only at publish time**, not at insert time
- This ensures prices that drop out of top-N can re-enter when better prices are removed

```
Initial allocation per ladder (bid or ask side):
┌──────────────────────────────────────────────────┐
│ level[0] │ level[1] │ level[2] │ ... │ level[N-1] │ (unused capacity)
└──────────────────────────────────────────────────┘
     ↑ best                             ↑ worst
```

### Worker Memory Layout

Each ladder owns its **own separate dynamic arrays**:

```
Private Worker:
  ladders[0]: bid_levels -> [dynamically allocated]
              ask_levels -> [dynamically allocated]
  ladders[1]: bid_levels -> [dynamically allocated]
              ask_levels -> [dynamically allocated]
  ...
  ladder_dirty: aligned_alloc(64, sub_count)  // 64-byte aligned

Public Worker:
  ladders[0]: bid_levels -> [dynamically allocated]
              ask_levels -> [dynamically allocated]
  ladders[1]: bid_levels -> [dynamically allocated]
              ask_levels -> [dynamically allocated]
  ...
  dirty: aligned_alloc(64, max_products)  // 64-byte aligned
```

### False Sharing Prevention

Each worker (private and public) has **completely separate memory allocations**:
- Per-ladder arrays are owned exclusively by that ladder
- `ladder_dirty` / `dirty` flags are 64-byte aligned, owned exclusively by one worker
- No shared counters or flags in the hot path between workers

### Benefits of Dynamic Arrays

1. **Correctness**: Prices outside top-N at insert time can become top-N later when other prices are removed.
2. **Cache efficiency**: Sorted arrays provide sequential access during publish.
3. **Simple logic**: No promotion scanning needed - all prices are tracked.
4. **Predictable publish**: `copy_full(max=N)` always returns exactly top-N in O(N) time.

### Sorted Array Operations

Price levels are maintained as **sorted arrays** (bids descending, asks ascending):

- **Insert**: O(log N) binary search + O(N) memmove. N is typically small (10-50 levels per product/side).
- **Remove**: O(log N) search + O(N) memmove.
- **Lookup**: O(log N) binary search in L1 cache.
- **Publish**: O(top_levels) sequential copy - **cache optimal**.

## Record Flow

1. The matcher writes a WAL record to disk.
2. The record pointer is enqueued to the 1P-NC ring buffer.
3. Workers dequeue in batches and aggregate for their orgs/products.

## Per-Record Aggregation Behavior

### INSERT (OmWalInsert)

**Public (product-level)**

- For the public worker assigned to the product:
  - Add `vol_remain` to the public ladder at `(product_id, side, price)`
  - **Only if** `price` qualifies for the current top-N range for that side.

**Private (org-level)**

- For each subscribed org in the **private** worker for this product:
  - Call `dealable(rec, viewer_org)` -> `dq`
  - If `dq > 0`, add `min(vol_remain, dq)` to the org's private ladder
  - **Only if** `price` qualifies for the current top-N range

The order is stored in per-org order maps and in the public order map so later
records can resolve price/product/remaining without rescanning the book.

### CANCEL / DEACTIVATE (OmWalCancel / OmWalDeactivate)

**Public**

- Lookup `order_id` in the public order map.
- If present and active, subtract remaining quantity from the public ladder
  at the stored `(product_id, side, price)`.

**Private**

- Each worker checks **all** its per-org order maps.
- For each org where the order exists, subtract remaining from that org's ladder.
- This ensures orders visible to multiple orgs are correctly removed from all.

**How a worker decides to process**

- A worker processes a cancel/deactivate **only if** the `order_id` exists in
  one of its per-org order maps (private), or in its public order map.
- If not found, the record is ignored by that worker (no subscription means no state).

### ACTIVATE (OmWalActivate)

**Public**

- Lookup in public order map; if inactive and remaining > 0, re-add remaining
  to public ladder.

**Private**

- For each per-org order map that contains the order, re-add remaining.

### MATCH (OmWalMatch)

**Public**

- Lookup maker order in public order map, subtract matched quantity.

**Private**

- For each per-org order map that contains the maker order, subtract matched quantity.

## Top-N Enforcement (At Publish Time)

- **ALL price levels** are aggregated and stored in the dynamic sorted arrays.
- Top-N filtering happens **only at publish time**, not at insert/remove time.
- When publishing:
  - Call `om_market_*_copy_full(worker, ..., out, top_levels)` to get the top N levels.
  - The sorted array is already in best-to-worst order, so copy is O(N).
  - Prices outside the top N are automatically excluded by the `max` parameter.
- This design ensures prices can move in and out of top-N dynamically as the order book changes.

## Publish Cadence

- Market snapshots are published at a fixed interval (e.g., 1s).
- If no WAL records affect a ladder, the **same snapshot** is still published.
- Dirty flags are used to skip re-serialization work when unchanged.

**Concrete publish loop (public):**

1. For each product assigned to the public worker, check `om_market_public_is_dirty()`.
2. If dirty, publish **delta** or **full** (see below) and then clear:
   - Delta: `om_market_public_clear_deltas()`
   - Full: `om_market_public_clear_dirty()`
3. If not dirty, publish cached data (or skip) based on your cadence requirements.

**Concrete publish loop (private):**

1. For each subscribed `(org_id, product_id)` pair in the worker, check
   `om_market_worker_is_dirty()`.
2. If dirty, publish **delta** or **full** (see below) and then clear:
   - Delta: `om_market_worker_clear_deltas()`
   - Full: `om_market_worker_clear_dirty()`
3. If not dirty, publish cached data (or skip) based on your cadence requirements.

## Delta vs Full Snapshot Publishing

- **Delta publishing (default):**
  - Only changed price levels are published each tick.
  - Backed by per-ladder delta maps (price -> delta).
  - Lowest bandwidth and fastest publish when changes are sparse.
  - **Public delta steps:**
    1. `count = om_market_public_delta_count(worker, product_id, side)`
    2. `n = om_market_public_copy_deltas(worker, product_id, side, out, max)`
    3. Serialize `out[0..n)` and publish to subscribers of `product_id`.
    4. `om_market_public_clear_deltas(worker, product_id, side)`
  - **Private delta steps:**
    1. `count = om_market_worker_delta_count(worker, org_id, product_id, side)`
    2. `n = om_market_worker_copy_deltas(worker, org_id, product_id, side, out, max)`
    3. Serialize `out[0..n)` and publish to that org.
    4. `om_market_worker_clear_deltas(worker, org_id, product_id, side)`

- **Full snapshot publishing (cache-optimal):**
  - Publishes top-N price levels every tick.
  - Use `om_market_worker_copy_full` / `om_market_public_copy_full` APIs.
  - **Pass `top_levels` as the `max` parameter** to get exactly top-N levels.
  - **Optimized for contiguous memory**: simple sequential array copy.
  - Higher bandwidth but simpler consumers and predictable latency.
  - **Public full steps:**
    1. `n = om_market_public_copy_full(worker, product_id, side, out, top_levels)`
    2. Serialize `out[0..n)` and publish to subscribers of `product_id`.
    3. `om_market_public_clear_dirty(worker, product_id)`
  - **Private full steps:**
    1. `n = om_market_worker_copy_full(worker, org_id, product_id, side, out, top_levels)`
    2. Serialize `out[0..n)` and publish to that org.
    3. `om_market_worker_clear_dirty(worker, org_id, product_id)`

## Performance Estimate (Order of Magnitude)

### Dynamic Sorted Array Performance

| Operation | Hash-based | Dynamic Sorted Array | Notes |
|-----------|------------|----------------------|-------|
| copy_full (publish) | ~200-500 ns | ~20-50 ns | **Sequential copy, cache optimal** |
| Price lookup | ~15-30 ns | ~10-20 ns | Binary search in L1 cache |
| Insert | ~20-40 ns | ~30-80 ns | O(log N) search + O(N) memmove |
| Remove | ~15-25 ns | ~20-50 ns | O(log N) search + O(N) memmove |
| Grow array | N/A | ~50-200 ns | Amortized O(1), rare |

The biggest win is in **full snapshot publishing**, which is a simple
sequential copy of contiguous memory. Insert/remove are slightly slower
due to memmove, but N is typically small (10-50 levels per product/side).

### Refactor impact (public/private split)

The public path is now **product-sharded**, so each WAL record updates **one** public ladder
instead of fanning out to many orgs. The private path remains **org-sharded** and still does
per-org fan-out for subscribed products. Practically:

- **Public cost per record** ~ `C_pub` (one order lookup + one ladder update).
- **Private cost per record** ~ `(avg_orgs_per_product / W_private) * C_priv`.

Total aggregation time is dominated by the private path unless the public worker count is too
small. Choose `W_public` so that `(R / W_public) * C_pub` stays below your publish window.

Let:

- `O` = number of orgs
- `P` = number of products (max 65,535)
- `S` = subscriptions per org
- `W` = number of worker threads
- `R` = WAL records/sec

Total subscriptions ~ `O * S`. Average orgs per product ~ `(O * S) / P`.
Per **private** worker, average orgs per product ~ `(O * S) / (P * W_private)`.

Example:

- `O = 5,000`
- `P = 65,535`
- `S = 10,000`

Total subs ~ 50,000,000 -> avg orgs per product ~ **~763**.
Per worker: **~763 / W** orgs per product.

If per-org update cost (dealable + ladder update) is ~0.2-0.5us (improved with contiguous memory):

- Per record **private** cost per worker ~ `(763 / W_private) * (0.2-0.5us)`
- Public cost per worker ~ `(R / W_public) * C_pub`
- Target is **2x realtime** (process 1s of WAL in <=0.5s)

For **R = 1,000,000 records/sec** (rough), the **minimum private** worker threads needed to meet
the <=0.5s aggregation window are approximately **3-10 workers** (improved from 4-13 with hash-based).

This range assumes per-org update cost of ~0.2-0.5us and the subscription
distribution above; it is an order-of-magnitude estimate.

## Performance Bottlenecks

1. **Per-org fan-out**: Inserts that must evaluate many orgs per product dominate CPU.
2. **Dealable callback cost**: Any heavy logic here multiplies by org fan-out.
3. **Order map lookups**: Order maps are still hash-based; cache misses possible for large maps.
4. **Publication serialization**: Even if no changes, formatting large snapshots can dominate.
5. **Delta map overhead**: Delta tracking still uses hash maps (sparse updates).

## Optimization Suggestions

1. **Set `expected_price_levels` appropriately**: This controls initial capacity for ladder arrays.
   Set it to the typical number of distinct price levels per product/side (e.g., 20-50).
2. **Keep top-N small** (e.g., 5-20) for publishing. The ladder tracks all prices, but publish
   only copies top-N which affects serialization bandwidth.
3. **Make dealable fast**: simple bit checks, precomputed org/product rules, no I/O.
4. **Batch WAL processing**: process 256-1024 records per batch to amortize overhead.
5. **Preallocate order maps**: size order maps to avoid rehash/resize.
6. **Use dense arrays if order_id is dense** for public order map (faster than hash).
7. **Dirty-flag publishing**: skip serialization if no change; still publish cached snapshot.
8. **Right-size public workers**: size `W_public` so each handles a stable product set; avoid
   a single public worker becoming a hotspot.
9. **Worker isolation**: each worker's dirty flags are 64-byte aligned and separately
   allocated to prevent false sharing between worker threads.
10. **Prefer full snapshot over delta** when most levels change each tick (simpler, cache-optimal).

These are order-of-magnitude estimates; actual throughput depends on dealable logic,
cache locality, and expected_price_levels settings.
