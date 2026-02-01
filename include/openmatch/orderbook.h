#ifndef OM_ORDERBOOK_H
#define OM_ORDERBOOK_H

#include "om_slab.h"
#include "om_hash.h"
#include <stdint.h>

/**
 * Orderbook context - manages all orderbooks across products
 * Contains the dual slab allocator, product book array, and order hashmap
 */
typedef struct OmOrderbookContext {
    OmDualSlab slab;                    /**< Dual slab allocator for all orders */
    OmProductBook products[OM_MAX_PRODUCTS]; /**< Array of product orderbooks */
    OmHashMap *order_hashmap;           /**< Hashmap: order_id -> OmOrderEntry */
    uint32_t next_slot_idx;             /**< Next slot index hint for Q0 */
} OmOrderbookContext;

/**
 * Initialize orderbook context
 * @param ctx Context to initialize
 * @param config Slab configuration
 * @return 0 on success, negative on error
 */
int om_orderbook_init(OmOrderbookContext *ctx, const OmSlabConfig *config);

/**
 * Destroy orderbook context and free all resources
 * @param ctx Context to destroy
 */
void om_orderbook_destroy(OmOrderbookContext *ctx);

/**
 * Insert order into orderbook after matching
 * Orders only enter the book after matching completes
 * Remaining volume is added to the book at the order's price level
 * 
 * @param ctx Orderbook context
 * @param product_id Product ID (0-65535)
 * @param slot Order slot (already populated with price, volume, flags, etc.)
 * @return 0 on success, negative on error
 * 
 * Operations:
 * - Find or create price level in Q1 (sorted by price)
 * - Append to time queue Q2 at that price level
 * - Add to org queue Q3
 * - Update product book head if new best price
 */
int om_orderbook_insert(OmOrderbookContext *ctx, uint16_t product_id, 
                        OmSlabSlot *slot);

/**
 * Cancel order from orderbook using order ID
 * Looks up order in hashmap (which contains product_id) and removes from all queues
 * 
 * @param ctx Orderbook context
 * @param order_id Order ID to cancel (as returned by om_slab_next_order_id)
 * @return true if cancelled, false if not found
 * 
 * Operations:
 * - Look up slot_idx and product_id from order_id in hashmap
 * - Remove from time queue Q2
 * - If last order at price level, remove price level from Q1
 * - Remove from org queue Q3
 * - Update product book head if removing best price
 * - Remove from hashmap
 * - Free slot back to slab
 */
bool om_orderbook_cancel(OmOrderbookContext *ctx, uint32_t order_id);

/**
 * Get best bid price for product (O(1))
 * @param ctx Orderbook context
 * @param product_id Product ID (0-65535)
 * @return Best bid price, 0 if no bids
 */
uint64_t om_orderbook_get_best_bid(const OmOrderbookContext *ctx, uint16_t product_id);

/**
 * Get best ask price for product (O(1))
 * @param ctx Orderbook context
 * @param product_id Product ID (0-65535)
 * @return Best ask price, 0 if no asks, or UINT64_MAX for market orders
 */
uint64_t om_orderbook_get_best_ask(const OmOrderbookContext *ctx, uint16_t product_id);

/**
 * Get total volume at a specific price level
 * Traverses Q2 at the given price level and sums volume_remain
 * 
 * @param ctx Orderbook context
 * @param product_id Product ID (0-65535)
 * @param price Price level to query
 * @param is_bid true for bid side, false for ask side
 * @return Total volume at price level, 0 if none
 */
uint64_t om_orderbook_get_volume_at_price(const OmOrderbookContext *ctx,
                                           uint16_t product_id, uint64_t price,
                                           bool is_bid);

/**
 * Get order slot by order ID using the internal hashmap
 * Only active orders (inserted into orderbook) are in the hashmap
 * 
 * @param ctx Orderbook context
 * @param order_id Order ID to look up
 * @return Slot pointer, or NULL if not found (order cancelled or never inserted)
 */
OmSlabSlot *om_orderbook_get_slot_by_id(OmOrderbookContext *ctx, uint32_t order_id);

/**
 * Check if price level exists in orderbook
 * @param ctx Orderbook context
 * @param product_id Product ID (0-65535)
 * @param price Price to check
 * @param is_bid true for bid side, false for ask side
 * @return true if price level exists
 */
bool om_orderbook_price_level_exists(const OmOrderbookContext *ctx,
                                      uint16_t product_id, uint64_t price,
                                      bool is_bid);

/**
 * Get count of price levels in orderbook for product
 * @param ctx Orderbook context  
 * @param product_id Product ID (0-65535)
 * @param is_bid true for bid side, false for ask side
 * @return Number of distinct price levels
 */
uint32_t om_orderbook_get_price_level_count(const OmOrderbookContext *ctx,
                                             uint16_t product_id, bool is_bid);

#endif
