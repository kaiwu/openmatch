# OpenMatch

Low-latency, single-threaded matching core in C11 with a cache-friendly slab
allocator, orderbook, and durable WAL replay. Built for HFT‑style workloads,
but small and embeddable.

## Features

- **Dual slab allocator** for fixed hot fields + separate aux (cold) data.
- **Orderbook** with price ladder (Q1) and time FIFO (Q2) queues.
- **Order ID hashmap** for O(1) cancel/lookup.
- **Write‑ahead log (WAL)** with CRC32 option, replay, and sequence recovery.
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
│   └── om_wal_mock.h         # WAL mock (prints to stderr)
├── src/                      # Implementations
├── tests/                    # check-based unit tests
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
- **Hashmap** maps `order_id → (slot_idx, product_id)` for O(1) cancel

API highlights:

- `om_orderbook_init()` / `om_orderbook_destroy()`
- `om_orderbook_insert()`
- `om_orderbook_cancel()`
- `om_orderbook_get_best_bid()` / `om_orderbook_get_best_ask()`

### WAL (`om_wal`)

Append-only log with replay support. Record types:

- `OM_WAL_INSERT` (variable length: fixed fields + user + aux data)
- `OM_WAL_CANCEL`
- `OM_WAL_MATCH`

Replay API:

- `om_wal_replay_init_with_config()`
- `om_wal_replay_next()` returns `-2` on CRC mismatch
- `om_orderbook_recover_from_wal()` reconstructs slab + orderbook

### Performance Presets (`om_perf`)

Presets include `OM_PERF_DEFAULT`, `OM_PERF_HFT`, `OM_PERF_DURABLE`,
`OM_PERF_RECOVERY`, `OM_PERF_MINIMAL`. Use `om_perf_validate()` and
`om_perf_autotune()` to verify/tune.

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
om_orderbook_init(&ctx, &slab_cfg, &wal);

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

1. **Matching engine** (price‑time priority, market/IOC/FOK behavior).
2. **Org queue (Q3)** implementation and org-level controls.
3. **Snapshotting** (state dump + WAL checkpointing).
4. **Async I/O** in WAL (flag exists, not implemented).
5. **Multi‑threading** (lock-free or sharded per-product orderbooks).
6. **Config wiring** from `om_perf` into live components.
7. **More validation** (parameter checks, error codes, and recovery guarantees).

## License

MIT
