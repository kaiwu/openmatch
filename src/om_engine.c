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

    if (config->wal) {
        engine->wal = malloc(sizeof(struct OmWal));
        if (!engine->wal) {
            return -3;
        }
        
        int wal_ret = om_wal_init(engine->wal, config->wal);
        if (wal_ret != 0) {
            free(engine->wal);
            engine->wal = NULL;
            return -3;
        }
        engine->wal_owned = true;
        wal_ptr = engine->wal;
    }

    int ob_ret = om_orderbook_init(&engine->orderbook, &config->slab, wal_ptr);
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

    bool taker_is_bid = OM_IS_BID(taker->flags);
    bool maker_is_bid = !taker_is_bid;

    OmOrderbookContext *book = &engine->orderbook;

    while (taker_remaining > 0) {
        OmSlabSlot *maker = om_orderbook_get_best_head(book, product_id, maker_is_bid);
        if (!maker) {
            break;
        }

        uint64_t maker_price = maker->price;
        uint64_t taker_price = taker->price;

        if (taker_is_bid) {
            if (taker_price < maker_price) {
                break;
            }
        } else {
            if (taker_price > maker_price) {
                break;
            }
        }

        uint64_t maker_remaining = maker->volume_remain;
        if (maker_remaining == 0) {
            om_orderbook_remove_slot(book, product_id, maker);
            continue;
        }

        uint64_t matchable = maker_remaining;
        if (taker_remaining < matchable) {
            matchable = taker_remaining;
        }

        if (engine->callbacks.can_match) {
            uint64_t allowed = engine->callbacks.can_match(maker, taker, engine->callbacks.user_ctx);
            if (allowed == 0) {
                break;
            }
            if (allowed < matchable) {
                matchable = allowed;
            }
        }

        if (matchable == 0) {
            break;
        }

        maker->volume_remain -= matchable;
        taker_remaining -= matchable;
        taker->volume_remain = taker_remaining;

        if (engine->callbacks.on_match) {
            engine->callbacks.on_match(maker, maker_price, matchable, engine->callbacks.user_ctx);
            engine->callbacks.on_match(taker, maker_price, matchable, engine->callbacks.user_ctx);
        }

        if (engine->callbacks.on_deal) {
            engine->callbacks.on_deal(maker, taker, maker_price, matchable, engine->callbacks.user_ctx);
        }

        if (engine->wal) {
            struct timespec ts;
            uint64_t ts_ns = 0;
            if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
                ts_ns = ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
            }

            OmWalMatch rec = {
                .maker_id = maker->order_id,
                .taker_id = taker->order_id,
                .price = maker_price,
                .volume = matchable,
                .timestamp_ns = ts_ns,
                .product_id = product_id,
                .reserved = {0, 0, 0}
            };
            om_wal_match(engine->wal, &rec);
        }

        if (maker->volume_remain == 0) {
            if (engine->callbacks.on_filled) {
                engine->callbacks.on_filled(maker, engine->callbacks.user_ctx);
            }
            om_orderbook_remove_slot(book, product_id, maker);
        }
    }

    if (taker_remaining == 0) {
        return 0;
    }

    if (engine->callbacks.pre_booked) {
        if (!engine->callbacks.pre_booked(taker, engine->callbacks.user_ctx)) {
            if (engine->callbacks.on_cancel) {
                engine->callbacks.on_cancel(taker, engine->callbacks.user_ctx);
            }
            return 0;
        }
    }

    if (engine->callbacks.on_booked) {
        engine->callbacks.on_booked(taker, engine->callbacks.user_ctx);
    }

    return om_orderbook_insert(book, product_id, taker);
}
