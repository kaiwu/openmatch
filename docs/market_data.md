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

- **Private workers** are **sharded by org**.
- **Public workers** are **sharded by product** (via `product_to_public_worker`).
- Private and public workers can consume the **same** WAL stream or different streams.
- Each worker maintains its own ladders and order maps (no cross-worker writes).

## Record Flow

1. The matcher writes a WAL record to disk.
2. The record pointer is enqueued to the 1P‑NC ring buffer.
3. Workers dequeue in batches and aggregate for their orgs/products.

## Per‑Record Aggregation Behavior

### INSERT (OmWalInsert)

**Public (product-level)**

- For the public worker assigned to the product:
  - Add `vol_remain` to the public ladder at `(product_id, side, price)`
  - **Only if** `price` is inside the current top‑N range for that side.

**Private (org-level)**

- For each subscribed org in the **private** worker for this product:
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
  - Backed by per‑ladder delta maps (price → delta).
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

- **Full snapshot publishing (optional):**
  - Publishes all top‑N price levels every tick.
  - Use `om_market_worker_copy_full` / `om_market_public_copy_full` APIs.
  - Higher bandwidth/CPU but simpler consumers.
  - **Public full steps:**
    1. `n = om_market_public_copy_full(worker, product_id, side, out, max)`
    2. Serialize `out[0..n)` and publish to subscribers of `product_id`.
    3. `om_market_public_clear_dirty(worker, product_id)`
  - **Private full steps:**
    1. `n = om_market_worker_copy_full(worker, org_id, product_id, side, out, max)`
    2. Serialize `out[0..n)` and publish to that org.
    3. `om_market_worker_clear_dirty(worker, org_id, product_id)`

## Performance Estimate (Order of Magnitude)

### Refactor impact (public/private split)

The public path is now **product‑sharded**, so each WAL record updates **one** public ladder
instead of fanning out to many orgs. The private path remains **org‑sharded** and still does
per‑org fan‑out for subscribed products. Practically:

- **Public cost per record** ≈ `C_pub` (one order lookup + one ladder update).
- **Private cost per record** ≈ `(avg_orgs_per_product / W_private) * C_priv`.

Total aggregation time is dominated by the private path unless the public worker count is too
small. Choose `W_public` so that `(R / W_public) * C_pub` stays below your publish window.

Let:

- `O` = number of orgs
- `P` = number of products (max 65,535)
- `S` = subscriptions per org
- `W` = number of worker threads
- `R` = WAL records/sec

Total subscriptions ≈ `O * S`. Average orgs per product ≈ `(O * S) / P`.
Per **private** worker, average orgs per product ≈ `(O * S) / (P * W_private)`.

Example:

- `O = 5,000`
- `P = 65,535`
- `S = 10,000`

Total subs ≈ 50,000,000 → avg orgs per product ≈ **~763**.
Per worker: **~763 / W** orgs per product.

If per‑org update cost (dealable + ladder update) is ~0.3–1.0µs:

- Per record **private** cost per worker ≈ `(763 / W_private) * (0.3–1.0µs)`
- Public cost per worker ≈ `(R / W_public) * C_pub`
- Target is **2× realtime** (process 1s of WAL in ≤0.5s)

For **R = 1,000,000 records/sec** (rough), the **minimum private** worker threads needed to meet
the ≤0.5s aggregation window are still approximately **4–13 workers**, because the private path
retains the per‑org fan‑out. Public workers can be sized independently based on `C_pub` and `R`.

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
7. **Right‑size public workers**: size `W_public` so each handles a stable product set; avoid
   a single public worker becoming a hotspot.
8. **Worker isolation**: avoid shared counters in the hot path to prevent cache line ping‑pong.

These are order‑of‑magnitude estimates; actual throughput depends on dealable logic,
cache locality, and top‑N settings.
