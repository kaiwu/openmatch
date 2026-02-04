# Market Data Aggregation (OpenMarket)

This document describes how OpenMarket consumes WAL records and builds
**public** and **private** market data per product/price/side.

## Definitions

- **Public ladder**: total remaining quantity at each price level for a product.
- **Private ladder**: dealable remaining quantity per org at each price level.
- **Top-N**: only the top `N` price levels are aggregated per side (bid/ask).
- **Dealable callback**: `uint64_t dealable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx)`
  returns the maximum dealable quantity for an org at insert time (0 = not dealable).

## Data Ownership & Sharding

- Workers are **sharded by org**.
- Each worker only processes products **subscribed** by its orgs.
- Each worker maintains its own ladders and order maps (no cross-worker writes).

## Record Flow

1. The matcher writes a WAL record to disk.
2. The record pointer is enqueued to the 1P‑NC ring buffer.
3. Workers dequeue in batches and aggregate for their orgs/products.

## Per‑Record Aggregation Behavior

### INSERT (OmWalInsert)

**Public (product-level)**

- If the product is subscribed by any org in this worker:
  - Add `vol_remain` to the public ladder at `(product_id, side, price)`
  - **Only if** `price` is inside the current top‑N range for that side.

**Private (org-level)**

- For each subscribed org in the worker for this product:
  - Call `dealable(rec, viewer_org)` → `dq`
  - If `dq > 0`, add `min(vol_remain, dq)` to the org’s private ladder
  - **Only if** `price` is inside the current top‑N range

The order is stored in per‑org order maps and in the public order map so later
records can resolve price/product/remaining without rescanning the book.

### CANCEL / DEACTIVATE (OmWalCancel / OmWalDeactivate)

**Public**

- Lookup `order_id` in the public order map.
- If present and active, subtract remaining quantity from the public ladder
  at the stored `(product_id, side, price)`.

**Private**

- Each worker checks only its own per‑org order maps.
- If the order exists for an org, subtract remaining from that org’s ladder.

**How a worker decides to process**

- A worker processes a cancel/deactivate **only if** the `order_id` exists in
  one of its per‑org order maps (private), or in its public order map.
- If not found, the record is ignored by that worker (no subscription means no state).

### ACTIVATE (OmWalActivate)

**Public**

- Lookup in public order map; if inactive and remaining > 0, re‑add remaining
  to public ladder.

**Private**

- For each per‑org order map that contains the order, re‑add remaining.

### MATCH (OmWalMatch)

**Public**

- Lookup maker order in public order map, subtract matched quantity.

**Private**

- For each per‑org order map that contains the maker order, subtract matched quantity.

## Top‑N Enforcement

- Aggregation only happens for the **top N price levels** per side.
- When adding a new price level, it is **ignored** if it falls outside the current top‑N.
- After an add, the ladder is trimmed to keep only top‑N.

## Publish Cadence

- Market snapshots are published at a fixed interval (e.g., 1s).
- If no WAL records affect a ladder, the **same snapshot** is still published.
- Dirty flags are used to skip re‑serialization work when unchanged.

## Performance Estimate (Order of Magnitude)

Let:

- `O` = number of orgs
- `P` = number of products (max 65,535)
- `S` = subscriptions per org
- `W` = number of worker threads
- `R` = WAL records/sec

Total subscriptions ≈ `O * S`. Average orgs per product ≈ `(O * S) / P`.
Per worker, average orgs per product ≈ `(O * S) / (P * W)`.

Example:

- `O = 5,000`
- `P = 65,535`
- `S = 10,000`

Total subs ≈ 50,000,000 → avg orgs per product ≈ **~763**.
Per worker: **~763 / W** orgs per product.

If per‑org update cost (dealable + ladder update) is ~0.3–1.0µs:

- Per record cost per worker ≈ `(763 / W) * (0.3–1.0µs)`
- Throughput per worker ≈ `1 / cost`
- Target is **2× realtime** (process 1s of WAL in ≤0.5s)

For **R = 1,000,000 records/sec** (rough), the **minimum** worker threads needed to meet
the ≤0.5s aggregation window are approximately **4–13 workers**.

This range assumes per‑org update cost of ~0.3–1.0µs and the subscription
distribution above; it is an order‑of‑magnitude estimate.

## Performance Bottlenecks

1. **Per‑org fan‑out**: Inserts that must evaluate many orgs per product dominate CPU.
2. **Dealable callback cost**: Any heavy logic here multiplies by org fan‑out.
3. **Hash map lookups**: Order maps and price maps are hot; cache misses are expensive.
4. **Top‑N enforcement**: Determining whether a new price is in the top‑N can be costly if done
   with full scans (optimize for small N).
5. **Publication serialization**: Even if no changes, formatting large snapshots can dominate.

## Optimization Suggestions

1. **Keep top‑N small** (e.g., 5–20). The cost grows with N.
2. **Make dealable fast**: simple bit checks, precomputed org/product rules, no I/O.
3. **Batch WAL processing**: process 256–1024 records per batch to amortize overhead.
4. **Preallocate maps**: size order maps and ladders to avoid rehash/resize.
5. **Use dense arrays if order_id is dense** for public order map (faster than hash).
6. **Dirty‑flag publishing**: skip serialization if no change; still publish cached snapshot.
7. **Worker isolation**: avoid shared counters in the hot path to prevent cache line ping‑pong.

These are order‑of‑magnitude estimates; actual throughput depends on dealable logic,
cache locality, and top‑N settings.
