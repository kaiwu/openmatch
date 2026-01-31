#ifndef OM_ENGINE_H
#define OM_ENGINE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include "om_slab.h"
#include "om_hash.h"

typedef enum {
    OM_SIDE_BID = 0,
    OM_SIDE_ASK = 1
} OmSide;

typedef enum {
    OM_ORDER_TYPE_LIMIT = 0,
    OM_ORDER_TYPE_MARKET = 1
} OmOrderType;

typedef struct OmOrder {
    uint64_t order_id;
    uint64_t price;
    uint32_t quantity;
    uint32_t remaining;
    OmSide side;
    OmOrderType type;
    uint64_t timestamp;
    uint64_t product_id;
    OmSlabSlot *slot;
} OmOrder;

typedef struct OmPriceLevel {
    uint64_t price;
    OmQueue orders;
    OmSlabSlot *level_slot;
} OmPriceLevel;

typedef struct OmProductBook {
    uint64_t product_id;
    OmHashMap *bid_levels;
    OmHashMap *ask_levels;
    OmDualSlab level_slab;
    OmDualSlab order_slab;
    uint64_t last_price;
    uint32_t last_quantity;
} OmProductBook;

typedef struct OmMatchResult {
    uint64_t maker_order_id;
    uint64_t taker_order_id;
    uint64_t price;
    uint32_t quantity;
    uint64_t timestamp;
} OmMatchResult;

typedef struct OmEngine {
    OmHashMap *products;
    OmDualSlab product_slab;
    uint64_t next_order_id;
    uint64_t timestamp_counter;
} OmEngine;

int om_engine_init(OmEngine *engine);
void om_engine_destroy(OmEngine *engine);

OmProductBook *om_engine_add_product(OmEngine *engine, uint64_t product_id);
OmProductBook *om_engine_get_product(OmEngine *engine, uint64_t product_id);
bool om_engine_remove_product(OmEngine *engine, uint64_t product_id);

int om_engine_place_order(OmEngine *engine, uint64_t product_id, OmOrder *order, OmMatchResult *results, size_t max_results);
bool om_engine_cancel_order(OmEngine *engine, uint64_t product_id, uint64_t order_id);

uint64_t om_engine_get_next_order_id(OmEngine *engine);

#endif
