#ifndef OM_ENGINE_H
#define OM_ENGINE_H

#include <stdint.h>
#include <stdbool.h>
#include "om_slab.h"
#include "orderbook.h"
#include "om_wal.h"

/**
 * @file om_engine.h
 * @brief Matching engine public API
 * 
 * The engine wraps the orderbook and provides a callback-based interface
 * for matching decisions. Actual matching logic is not implemented here -
 * this is scaffolding for future matching behavior.
 */

/* Forward declarations */
struct OmSlabSlot;
struct OmWal;

/**
 * Matching callback type
 * Called during matching to determine if two orders can match
 * 
 * @param maker The resting order in the book (passive side)
 * @param taker The incoming order (aggressive side)
 * @param user_ctx User-provided context pointer
 * @return Volume that can be matched (0 = no match, >0 = match volume)
 * 
 * Example use cases:
 * - Reject self-matching (same org)
 * - Implement custom matching rules
 * - Rate limiting or circuit breakers
 */
typedef uint64_t (*OmCanMatchFn)(const OmSlabSlot *maker, 
                                  const OmSlabSlot *taker, 
                                  void *user_ctx);

/**
 * Match callback type
 * Called when a trade is executed for a single order (maker or taker)
 *
 * @param order The order being filled
 * @param price Execution price
 * @param qty Execution quantity for this callback
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnMatchFn)(const OmSlabSlot *order,
                            uint64_t price,
                            uint64_t qty,
                            void *user_ctx);

/**
 * Deal callback type
 * Called once per trade with both maker and taker
 *
 * @param maker The resting order in the book (passive side)
 * @param taker The incoming order (aggressive side)
 * @param price Execution price
 * @param qty Execution quantity
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnDealFn)(const OmSlabSlot *maker,
                           const OmSlabSlot *taker,
                           uint64_t price,
                           uint64_t qty,
                           void *user_ctx);

/**
 * Booked callback type
 * Called when a taker order rests on the book
 *
 * @param order The order that was booked
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnBookedFn)(const OmSlabSlot *order, void *user_ctx);

/**
 * Filled callback type
 * Called when a maker order is fully filled and removed from the book
 *
 * @param order The order that was fully filled
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnFilledFn)(const OmSlabSlot *order, void *user_ctx);

/**
 * Cancel callback type
 * Called when a maker order is voluntarily cancelled and removed from the book
 *
 * @param order The order that was cancelled
 * @param user_ctx User-provided context pointer
 */
typedef void (*OmOnCancelFn)(const OmSlabSlot *order, void *user_ctx);

/**
 * Pre-booked callback type
 * Called before booking a taker order that has remaining quantity
 * Return true to allow booking, false to cancel remaining quantity.
 *
 * @param order The taker order to be booked
 * @param user_ctx User-provided context pointer
 * @return true to book, false to cancel remaining qty
 */
typedef bool (*OmPreBookedFn)(const OmSlabSlot *order, void *user_ctx);

/**
 * Engine callbacks configuration
 * Contains all user-defined callbacks for engine behavior
 */
typedef struct OmEngineCallbacks {
    OmCanMatchFn can_match;     /**< Optional matching predicate callback */
    OmOnMatchFn on_match;        /**< Optional match notification callback */
    OmOnDealFn on_deal;           /**< Optional deal notification callback */
    OmOnBookedFn on_booked;       /**< Optional booked notification callback */
    OmOnFilledFn on_filled;       /**< Optional filled notification callback */
    OmOnCancelFn on_cancel;        /**< Optional cancel notification callback */
    OmPreBookedFn pre_booked;      /**< Optional pre-booking decision callback */
    void *user_ctx;             /**< User context passed to callbacks */
} OmEngineCallbacks;

/**
 * Engine configuration
 * Combines slab, WAL, and callback configuration
 */
typedef struct OmEngineConfig {
    OmSlabConfig slab;          /**< Slab allocator configuration (required) */
    OmWalConfig *wal;           /**< Optional WAL configuration (NULL to disable) */
    OmEngineCallbacks callbacks; /**< Callback configuration */
} OmEngineConfig;

/**
 * Matching engine context
 * Wraps the orderbook and stores callback configuration
 */
typedef struct OmEngine {
    OmOrderbookContext orderbook; /**< Internal orderbook (slab, products, hashmap) */
    OmEngineCallbacks callbacks;   /**< Stored callback configuration */
    struct OmWal *wal;            /**< WAL pointer (owned if config provided, NULL otherwise) */
    bool wal_owned;               /**< true if engine allocated WAL internally */
} OmEngine;

/**
 * Initialize matching engine
 * 
 * Initializes the orderbook with the provided slab and WAL configuration.
 * Stores callback pointers for later use during matching.
 * 
 * @param engine Engine context to initialize
 * @param config Engine configuration (slab, optional WAL, callbacks)
 * @return 0 on success, negative on error
 * 
 * Error codes:
 *   -1: Invalid parameters (NULL engine or config)
 *   -2: Slab initialization failed
 *   -3: WAL initialization failed
 *   -4: Orderbook initialization failed
 */
int om_engine_init(OmEngine *engine, const OmEngineConfig *config);

/**
 * Destroy matching engine and free all resources
 * 
 * Destroys the orderbook, closes the WAL (if owned), and clears callback pointers.
 * Safe to call on a zeroed or partially initialized engine.
 * 
 * @param engine Engine context to destroy
 */
void om_engine_destroy(OmEngine *engine);

/**
 * Get the orderbook context from engine
 * 
 * Useful for direct orderbook operations (insert, cancel, query).
 * 
 * @param engine Engine context
 * @return Pointer to orderbook context, or NULL if engine is NULL
 */
static inline OmOrderbookContext *om_engine_get_orderbook(OmEngine *engine) {
    return engine ? &engine->orderbook : NULL;
}

/**
 * Get the WAL context from engine
 * 
 * @param engine Engine context
 * @return Pointer to WAL, or NULL if WAL disabled or engine is NULL
 */
static inline struct OmWal *om_engine_get_wal(OmEngine *engine) {
    return engine ? engine->wal : NULL;
}

/**
 * Check if a matching callback is configured
 * 
 * @param engine Engine context
 * @return true if can_match callback is set
 */
static inline bool om_engine_has_can_match(const OmEngine *engine) {
    return engine && engine->callbacks.can_match != NULL;
}

/**
 * Check if a match callback is configured
 *
 * @param engine Engine context
 * @return true if on_match callback is set
 */
static inline bool om_engine_has_on_match(const OmEngine *engine) {
    return engine && engine->callbacks.on_match != NULL;
}

/**
 * Check if a deal callback is configured
 *
 * @param engine Engine context
 * @return true if on_deal callback is set
 */
static inline bool om_engine_has_on_deal(const OmEngine *engine) {
    return engine && engine->callbacks.on_deal != NULL;
}

/**
 * Check if a booked callback is configured
 *
 * @param engine Engine context
 * @return true if on_booked callback is set
 */
static inline bool om_engine_has_on_booked(const OmEngine *engine) {
    return engine && engine->callbacks.on_booked != NULL;
}

/**
 * Check if a filled callback is configured
 *
 * @param engine Engine context
 * @return true if on_filled callback is set
 */
static inline bool om_engine_has_on_filled(const OmEngine *engine) {
    return engine && engine->callbacks.on_filled != NULL;
}

/**
 * Check if a cancel callback is configured
 *
 * @param engine Engine context
 * @return true if on_cancel callback is set
 */
static inline bool om_engine_has_on_cancel(const OmEngine *engine) {
    return engine && engine->callbacks.on_cancel != NULL;
}

/**
 * Check if a pre-booked callback is configured
 *
 * @param engine Engine context
 * @return true if pre_booked callback is set
 */
static inline bool om_engine_has_pre_booked(const OmEngine *engine) {
    return engine && engine->callbacks.pre_booked != NULL;
}

/**
 * Match an incoming order against the orderbook
 *
 * @param engine Engine context
 * @param product_id Product ID
 * @param taker Incoming order slot
 * @return 0 on success, negative on error
 */
int om_engine_match(OmEngine *engine, uint16_t product_id, OmSlabSlot *taker);

/**
 * Deactivate a resting order and remove it from the book without freeing it
 *
 * @param engine Engine context
 * @param order_id Order ID to deactivate
 * @return true if deactivated, false if not found or not active
 */
bool om_engine_deactivate(OmEngine *engine, uint32_t order_id);

/**
 * Activate a previously deactivated order and re-run matching as taker
 *
 * @param engine Engine context
 * @param order_id Order ID to activate
 * @return true if activated, false if not found or not deactivated
 */
bool om_engine_activate(OmEngine *engine, uint32_t order_id);

#endif /* OM_ENGINE_H */
