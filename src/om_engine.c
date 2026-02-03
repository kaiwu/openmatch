#include "openmatch/om_engine.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

int om_engine_init(OmEngine *engine, const OmEngineConfig *config)
{
    if (!engine || !config) {
        return -1;
    }

    memset(engine, 0, sizeof(OmEngine));

    struct OmWal *wal_ptr = NULL;
    OmSlabConfig slab_cfg = config->slab;
    OmWalConfig *wal_cfg = config->wal;
    OmWalConfig wal_cfg_local;
    const OmPerfConfig *perf = config->perf;

    uint32_t max_products = config->max_products;
    uint32_t max_org = config->max_org;
    uint32_t hashmap_cap = config->hashmap_initial_cap;

    if (perf) {
        slab_cfg.user_data_size = perf->slab_user_data_size;
        slab_cfg.aux_data_size = perf->slab_aux_data_size;
        slab_cfg.total_slots = perf->slab_total_slots;

        if (hashmap_cap == 0) {
            hashmap_cap = perf->hashmap_initial_cap;
        }

        if (wal_cfg) {
            wal_cfg_local = *wal_cfg;
            wal_cfg_local.buffer_size = perf->wal_buffer_size;
            wal_cfg_local.sync_interval_ms = perf->wal_sync_interval_ms;
            wal_cfg_local.use_direct_io = perf->wal_use_direct_io;
            wal_cfg_local.enable_crc32 = perf->wal_enable_crc32;
            wal_cfg_local.user_data_size = slab_cfg.user_data_size;
            wal_cfg_local.aux_data_size = slab_cfg.aux_data_size;
            wal_cfg = &wal_cfg_local;
        }
    }

    if (max_products == 0 || max_org == 0) {
        return -1;
    }

    if (hashmap_cap == 0) {
        hashmap_cap = slab_cfg.total_slots;
    }

    if (wal_cfg) {
        engine->wal = malloc(sizeof(struct OmWal));
        if (!engine->wal) {
            return -3;
        }
        
        int wal_ret = om_wal_init(engine->wal, wal_cfg);
        if (wal_ret != 0) {
            free(engine->wal);
            engine->wal = NULL;
            return -3;
        }
        engine->wal_owned = true;
        wal_ptr = engine->wal;
    }

    int ob_ret = om_orderbook_init(&engine->orderbook, &slab_cfg, wal_ptr,
                                   max_products, max_org, hashmap_cap);
    if (ob_ret != 0) {
        if (engine->wal_owned && engine->wal) {
            om_wal_close(engine->wal);
            free(engine->wal);
            engine->wal = NULL;
        }
        return -4;
    }

    engine->callbacks = config->callbacks;

    return 0;
}

int om_engine_init_perf(OmEngine *engine, const OmEngineConfig *config, const OmPerfConfig *perf)
{
    if (!config) {
        return -1;
    }
    OmEngineConfig cfg = *config;
    cfg.perf = perf;
    return om_engine_init(engine, &cfg);
}

void om_engine_destroy(OmEngine *engine)
{
    if (!engine) {
        return;
    }

    om_orderbook_destroy(&engine->orderbook);

    if (engine->wal_owned && engine->wal) {
        om_wal_close(engine->wal);
        free(engine->wal);
    }

    memset(engine, 0, sizeof(OmEngine));
}

int om_engine_match(OmEngine *engine, uint16_t product_id, OmSlabSlot *taker)
{
    if (!engine || !taker) {
        return -1;
    }

    uint64_t taker_remaining = taker->volume_remain;
    if (taker_remaining == 0) {
        return 0;
    }

    const bool taker_is_bid = OM_IS_BID(taker->flags);
    const bool maker_is_bid = !taker_is_bid;
    const uint64_t taker_price = taker->price;

    OmOrderbookContext *book = &engine->orderbook;
    OmEngineCallbacks *cb = &engine->callbacks;
    OmWal *wal = engine->wal;
    const bool has_can_match = cb->can_match != NULL;
    const bool has_on_match = cb->on_match != NULL;
    const bool has_on_deal = cb->on_deal != NULL;
    const bool has_on_filled = cb->on_filled != NULL;
    const bool has_pre_booked = cb->pre_booked != NULL;
    const bool has_on_booked = cb->on_booked != NULL;
    const bool has_on_cancel = cb->on_cancel != NULL;

    uint64_t match_ts_ns = 0;
    if (wal) {
        struct timespec ts;
        if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
            match_ts_ns = ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
        }
    }

    OmSlabSlot *level = om_orderbook_get_best_head(book, product_id, maker_is_bid);
    uint32_t level_idx = level ? om_slot_get_idx(&book->slab, level) : OM_SLOT_IDX_NULL;

    while (taker_remaining > 0 && level_idx != OM_SLOT_IDX_NULL) {
        level = om_slot_from_idx(&book->slab, level_idx);
        if (!level) {
            break;
        }

        uint64_t level_price = level->price;

        if (taker_is_bid) {
            if (taker_price < level_price) {
                break;
            }
        } else {
            if (taker_price > level_price) {
                break;
            }
        }

        uint32_t next_level_idx = level->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;
        uint32_t maker_idx = level_idx;

        while (maker_idx != OM_SLOT_IDX_NULL && taker_remaining > 0) {
            OmSlabSlot *maker = om_slot_from_idx(&book->slab, maker_idx);
            if (!maker) {
                break;
            }

            uint32_t next_maker_idx = maker->queue_nodes[OM_Q2_TIME_FIFO].next_idx;

            uint64_t maker_remaining = maker->volume_remain;
            if (maker_remaining == 0) {
                om_orderbook_remove_slot(book, product_id, maker);
                maker_idx = next_maker_idx;
                continue;
            }

            uint64_t matchable = maker_remaining;
            if (taker_remaining < matchable) {
                matchable = taker_remaining;
            }

            if (has_can_match) {
                uint64_t allowed = cb->can_match(maker, taker, cb->user_ctx);
                if (allowed == 0) {
                    maker_idx = next_maker_idx;
                    continue;
                }
                if (allowed < matchable) {
                    matchable = allowed;
                }
            }

            if (matchable == 0) {
                maker_idx = next_maker_idx;
                continue;
            }

            maker->volume_remain -= matchable;
            taker_remaining -= matchable;
            taker->volume_remain = taker_remaining;

            if (has_on_match) {
                cb->on_match(maker, level_price, matchable, cb->user_ctx);
                cb->on_match(taker, level_price, matchable, cb->user_ctx);
            }

            if (has_on_deal) {
                cb->on_deal(maker, taker, level_price, matchable, cb->user_ctx);
            }

            if (wal) {
                OmWalMatch rec = {
                    .maker_id = maker->order_id,
                    .taker_id = taker->order_id,
                    .price = level_price,
                    .volume = matchable,
                    .timestamp_ns = match_ts_ns,
                    .product_id = product_id,
                    .reserved = {0, 0, 0}
                };
                om_wal_match(wal, &rec);
            }

            if (maker->volume_remain == 0) {
                if (has_on_filled) {
                    cb->on_filled(maker, cb->user_ctx);
                }
                om_orderbook_remove_slot(book, product_id, maker);
                maker_idx = next_maker_idx;
                continue;
            }

            if (taker_remaining == 0) {
                break;
            }

            continue;
        }

        if (taker_remaining == 0) {
            break;
        }

        level_idx = next_level_idx;
    }

    if (taker_remaining == 0) {
        return 0;
    }

    if (has_pre_booked) {
        if (!cb->pre_booked(taker, cb->user_ctx)) {
            if (has_on_cancel) {
                cb->on_cancel(taker, cb->user_ctx);
            }
            return 0;
        }
    }

    if (has_on_booked) {
        cb->on_booked(taker, cb->user_ctx);
    }

    return om_orderbook_insert(book, product_id, taker);
}

bool om_engine_cancel(OmEngine *engine, uint32_t order_id)
{
    if (!engine) {
        return false;
    }

    OmOrderbookContext *book = &engine->orderbook;
    OmOrderEntry *entry = om_hash_get(book->order_hashmap, order_id);
    if (!entry) {
        return false;
    }

    OmSlabSlot *order = om_slot_from_idx(&book->slab, entry->slot_idx);
    if (!order) {
        return false;
    }

    if (engine->callbacks.on_cancel) {
        engine->callbacks.on_cancel(order, engine->callbacks.user_ctx);
    }

    return om_orderbook_cancel(book, order_id);
}

bool om_engine_deactivate(OmEngine *engine, uint32_t order_id)
{
    if (!engine) {
        return false;
    }

    OmOrderEntry *entry = om_hash_get(engine->orderbook.order_hashmap, order_id);
    if (!entry) {
        return false;
    }

    OmSlabSlot *order = om_slot_from_idx(&engine->orderbook.slab, entry->slot_idx);
    if (!order) {
        return false;
    }

    if ((order->flags & OM_STATUS_MASK) == OM_STATUS_DEACTIVATED) {
        return false;
    }

    if (!om_orderbook_unlink_slot(&engine->orderbook, entry->product_id, order)) {
        return false;
    }

    order->flags = OM_SET_STATUS(order->flags, OM_STATUS_DEACTIVATED);

    if (engine->wal) {
        om_wal_deactivate(engine->wal, order_id, entry->slot_idx, entry->product_id);
    }

    return true;
}

bool om_engine_activate(OmEngine *engine, uint32_t order_id)
{
    if (!engine) {
        return false;
    }

    OmOrderEntry *entry = om_hash_get(engine->orderbook.order_hashmap, order_id);
    if (!entry) {
        return false;
    }

    OmSlabSlot *order = om_slot_from_idx(&engine->orderbook.slab, entry->slot_idx);
    if (!order) {
        return false;
    }

    if ((order->flags & OM_STATUS_MASK) != OM_STATUS_DEACTIVATED) {
        return false;
    }

    order->flags = OM_SET_STATUS(order->flags, OM_STATUS_NEW);

    if (engine->wal) {
        om_wal_activate(engine->wal, order_id, entry->slot_idx, entry->product_id);
    }

    return om_engine_match(engine, entry->product_id, order) == 0;
}

uint32_t om_engine_cancel_org_product(OmEngine *engine, uint16_t product_id, uint16_t org_id)
{
    if (!engine) {
        return 0;
    }
    return om_orderbook_cancel_org_product(&engine->orderbook, product_id, org_id);
}

uint32_t om_engine_cancel_org_all(OmEngine *engine, uint16_t org_id)
{
    if (!engine) {
        return 0;
    }
    return om_orderbook_cancel_org_all(&engine->orderbook, org_id);
}

uint32_t om_engine_cancel_product_side(OmEngine *engine, uint16_t product_id, bool is_bid)
{
    if (!engine) {
        return 0;
    }

    OmOrderbookContext *book = &engine->orderbook;
    if (product_id >= book->max_products) {
        return 0;
    }

    OmEngineCallbacks *cb = &engine->callbacks;
    const bool has_on_cancel = cb->on_cancel != NULL;

    uint32_t cancelled = 0;
    uint32_t level_idx = is_bid ? book->products[product_id].bid_head_q1
                                : book->products[product_id].ask_head_q1;

    while (level_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *level = om_slot_from_idx(&book->slab, level_idx);
        if (!level) {
            break;
        }
        uint32_t next_level_idx = level->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;

        uint32_t order_idx = level_idx;
        while (order_idx != OM_SLOT_IDX_NULL) {
            OmSlabSlot *order = om_slot_from_idx(&book->slab, order_idx);
            if (!order) {
                break;
            }
            uint32_t next_order_idx = order->queue_nodes[OM_Q2_TIME_FIFO].next_idx;
            if (has_on_cancel) {
                cb->on_cancel(order, cb->user_ctx);
            }
            if (om_orderbook_cancel(book, order->order_id)) {
                cancelled++;
            }
            order_idx = next_order_idx;
        }

        level_idx = next_level_idx;
    }

    return cancelled;
}

uint32_t om_engine_cancel_product(OmEngine *engine, uint16_t product_id)
{
    if (!engine) {
        return 0;
    }

    uint32_t cancelled = 0;
    cancelled += om_engine_cancel_product_side(engine, product_id, true);
    cancelled += om_engine_cancel_product_side(engine, product_id, false);
    return cancelled;
}
