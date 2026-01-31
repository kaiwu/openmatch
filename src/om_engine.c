#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "openmatch/om_engine.h"

int om_engine_init(OmEngine *engine) {
    if (!engine) return -1;

    memset(engine, 0, sizeof(OmEngine));

    engine->products = om_hash_create(16);
    if (!engine->products) {
        return -1;
    }

    if (om_slab_init(&engine->product_slab, sizeof(OmProductBook)) != 0) {
        om_hash_destroy(engine->products);
        return -1;
    }

    engine->next_order_id = 1;
    engine->timestamp_counter = 0;

    return 0;
}

void om_engine_destroy(OmEngine *engine) {
    if (!engine) return;

    if (engine->products) {
        om_hash_destroy(engine->products);
    }

    om_slab_destroy(&engine->product_slab);
    memset(engine, 0, sizeof(OmEngine));
}

OmProductBook *om_engine_add_product(OmEngine *engine, uint64_t product_id) {
    if (!engine) return NULL;

    if (om_hash_contains(engine->products, product_id)) {
        return NULL;
    }

    OmSlabSlot *slot = om_slab_alloc(&engine->product_slab);
    if (!slot) {
        return NULL;
    }

    OmProductBook *book = (OmProductBook *)om_slot_get_data(slot);
    memset(book, 0, sizeof(OmProductBook));

    book->product_id = product_id;
    book->bid_levels = om_hash_create(16);
    book->ask_levels = om_hash_create(16);

    if (!book->bid_levels || !book->ask_levels) {
        om_slab_free(&engine->product_slab, slot);
        return NULL;
    }

    if (om_slab_init(&book->level_slab, sizeof(OmPriceLevel)) != 0) {
        om_hash_destroy(book->bid_levels);
        om_hash_destroy(book->ask_levels);
        om_slab_free(&engine->product_slab, slot);
        return NULL;
    }

    if (om_slab_init(&book->order_slab, sizeof(OmOrder)) != 0) {
        om_slab_destroy(&book->level_slab);
        om_hash_destroy(book->bid_levels);
        om_hash_destroy(book->ask_levels);
        om_slab_free(&engine->product_slab, slot);
        return NULL;
    }

    if (!om_hash_insert(engine->products, product_id, book)) {
        om_slab_destroy(&book->order_slab);
        om_slab_destroy(&book->level_slab);
        om_hash_destroy(book->bid_levels);
        om_hash_destroy(book->ask_levels);
        om_slab_free(&engine->product_slab, slot);
        return NULL;
    }

    return book;
}

OmProductBook *om_engine_get_product(OmEngine *engine, uint64_t product_id) {
    if (!engine) return NULL;
    return (OmProductBook *)om_hash_get(engine->products, product_id);
}

bool om_engine_remove_product(OmEngine *engine, uint64_t product_id) {
    if (!engine) return false;

    OmProductBook *book = om_engine_get_product(engine, product_id);
    if (!book) return false;

    om_slab_destroy(&book->order_slab);
    om_slab_destroy(&book->level_slab);
    om_hash_destroy(book->bid_levels);
    om_hash_destroy(book->ask_levels);

    om_hash_remove(engine->products, product_id);

    return true;
}

uint64_t om_engine_get_next_order_id(OmEngine *engine) {
    if (!engine) return 0;
    return engine->next_order_id++;
}

static inline OmHashMap *get_side_levels(OmProductBook *book, OmSide side) {
    return side == OM_SIDE_BID ? book->bid_levels : book->ask_levels;
}

static inline OmPriceLevel *find_or_create_level(OmProductBook *book, uint64_t price, OmSide side) {
    OmHashMap *levels = get_side_levels(book, side);
    OmPriceLevel *level = om_hash_get(levels, price);

    if (level) return level;

    OmSlabSlot *slot = om_slab_alloc(&book->level_slab);
    if (!slot) return NULL;

    level = (OmPriceLevel *)om_slot_get_data(slot);
    memset(level, 0, sizeof(OmPriceLevel));
    level->price = price;
    level->level_slot = slot;
    om_queue_init(&level->orders, 0);

    if (!om_hash_insert(levels, price, level)) {
        om_slab_free(&book->level_slab, slot);
        return NULL;
    }

    return level;
}

static inline uint64_t get_timestamp(OmEngine *engine) {
    return ++engine->timestamp_counter;
}

static int match_order(OmEngine *engine, OmProductBook *book, OmOrder *order,
                       OmMatchResult *results, size_t max_results, size_t *result_count) {
    (void)engine;
    OmHashMap *opposite_levels = (order->side == OM_SIDE_BID) ? book->ask_levels : book->bid_levels;

    while (order->remaining > 0 && *result_count < max_results) {
        khiter_t k;
        OmPriceLevel *best_level = NULL;
        uint64_t best_price = 0;

        for (k = kh_begin(opposite_levels->hash); k != kh_end(opposite_levels->hash); ++k) {
            if (!kh_exist(opposite_levels->hash, k)) continue;
            OmPriceLevel *level = kh_value(opposite_levels->hash, k);
            if (!best_level) {
                best_level = level;
                best_price = level->price;
            } else {
                if (order->side == OM_SIDE_BID) {
                    if (level->price < best_price) {
                        best_level = level;
                        best_price = level->price;
                    }
                } else {
                    if (level->price > best_price) {
                        best_level = level;
                        best_price = level->price;
                    }
                }
            }
        }

        if (!best_level) break;

        if (order->side == OM_SIDE_BID && best_price > order->price) break;
        if (order->side == OM_SIDE_ASK && best_price < order->price) break;

        while (order->remaining > 0 && *result_count < max_results && !om_queue_is_empty(&best_level->orders)) {
            OmSlabSlot *maker_slot = om_queue_pop(&best_level->orders, 0);
            if (!maker_slot) break;

            OmOrder *maker = (OmOrder *)om_slot_get_data(maker_slot);
            uint32_t match_qty = (order->remaining < maker->remaining) ? order->remaining : maker->remaining;

            OmMatchResult *result = &results[(*result_count)++];
            result->maker_order_id = maker->order_id;
            result->taker_order_id = order->order_id;
            result->price = best_price;
            result->quantity = match_qty;
            result->timestamp = get_timestamp(engine);

            order->remaining -= match_qty;
            maker->remaining -= match_qty;

            if (maker->remaining > 0) {
                om_queue_push(&best_level->orders, maker_slot, 0);
            } else {
                om_slab_free(&book->order_slab, maker_slot);
            }
        }

        if (om_queue_is_empty(&best_level->orders)) {
            om_hash_remove(opposite_levels, best_level->price);
            om_slab_free(&book->level_slab, best_level->level_slot);
        }
    }

    return 0;
}

int om_engine_place_order(OmEngine *engine, uint64_t product_id, OmOrder *order,
                          OmMatchResult *results, size_t max_results) {
    if (!engine || !order || !results || max_results == 0) return -1;

    OmProductBook *book = om_engine_get_product(engine, product_id);
    if (!book) {
        book = om_engine_add_product(engine, product_id);
        if (!book) return -1;
    }

    size_t result_count = 0;
    order->order_id = om_engine_get_next_order_id(engine);
    order->timestamp = get_timestamp(engine);
    order->remaining = order->quantity;
    order->product_id = product_id;

    if (order->type == OM_ORDER_TYPE_MARKET || order->price == 0) {
        match_order(engine, book, order, results, max_results, &result_count);
    } else {
        match_order(engine, book, order, results, max_results, &result_count);

        if (order->remaining > 0) {
            OmPriceLevel *level = find_or_create_level(book, order->price, order->side);
            if (!level) return -1;

            OmSlabSlot *slot = om_slab_alloc(&book->order_slab);
            if (!slot) return -1;

            OmOrder *stored = (OmOrder *)om_slot_get_data(slot);
            memcpy(stored, order, sizeof(OmOrder));
            stored->slot = slot;

            om_queue_push(&level->orders, slot, 0);
        }
    }

    if (result_count > 0) {
        book->last_price = results[result_count - 1].price;
        book->last_quantity = results[result_count - 1].quantity;
    }

    return (int)result_count;
}

bool om_engine_cancel_order(OmEngine *engine, uint64_t product_id, uint64_t order_id) {
    (void)engine; (void)product_id; (void)order_id;
    return false;
}
