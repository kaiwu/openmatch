#include "orderbook.h"
#include <string.h>

int om_orderbook_init(OmOrderbookContext *ctx, const OmSlabConfig *config)
{
    int ret = om_slab_init(&ctx->slab, config);
    if (ret != 0) {
        return ret;
    }

    /* Initialize all product books to empty state */
    for (int i = 0; i < OM_MAX_PRODUCTS; i++) {
        ctx->products[i].bid_head_q1 = OM_SLOT_IDX_NULL;
        ctx->products[i].ask_head_q1 = OM_SLOT_IDX_NULL;
    }

    /* Create order hashmap with initial capacity matching slab */
    ctx->order_hashmap = om_hash_create(config->total_slots);
    if (!ctx->order_hashmap) {
        om_slab_destroy(&ctx->slab);
        return -1;
    }

    ctx->next_slot_idx = 0;
    return 0;
}

void om_orderbook_destroy(OmOrderbookContext *ctx)
{
    if (ctx->order_hashmap) {
        om_hash_destroy(ctx->order_hashmap);
        ctx->order_hashmap = NULL;
    }
    om_slab_destroy(&ctx->slab);
}

/**
 * Find price level in Q1 for given product and price
 * Returns pointer to price level slot if found, NULL if not found
 * When not found, *insert_after is set to the node after which to insert
 * (NULL means insert at head, otherwise insert after this node)
 */
static OmSlabSlot *find_price_level_with_insertion_point(OmOrderbookContext *ctx, 
                                                          uint16_t product_id,
                                                          uint64_t price, 
                                                          bool is_bid,
                                                          OmSlabSlot **insert_after)
{
    OmProductBook *book = &ctx->products[product_id];
    uint32_t head_idx = is_bid ? book->bid_head_q1 : book->ask_head_q1;

    if (head_idx == OM_SLOT_IDX_NULL) {
        *insert_after = NULL;
        return NULL;
    }

    OmSlabSlot *head = om_slot_from_idx(&ctx->slab, head_idx);
    
    /* Check if we need to insert at head (new best price) */
    if (is_bid && price > head->price) {
        *insert_after = NULL;  /* Insert at head */
        return NULL;
    }
    if (!is_bid && price < head->price) {
        *insert_after = NULL;  /* Insert at head */
        return NULL;
    }

    /* Scan through Q1 price ladder */
    uint32_t curr_idx = head_idx;
    OmSlabSlot *curr = head;
    OmSlabSlot *prev = NULL;

    while (curr_idx != OM_SLOT_IDX_NULL) {
        if (curr->price == price) {
            *insert_after = NULL;  /* Not used when found */
            return curr;  /* Found exact price level */
        }

        /* For bids: scan descending, stop when price < target
         * For asks: scan ascending, stop when price > target
         */
        if (is_bid && curr->price < price) {
            *insert_after = prev;  /* Insert after previous */
            return NULL;
        }
        if (!is_bid && curr->price > price) {
            *insert_after = prev;  /* Insert after previous */
            return NULL;
        }

        prev = curr;
        curr_idx = curr->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;
        if (curr_idx != OM_SLOT_IDX_NULL) {
            curr = om_slot_from_idx(&ctx->slab, curr_idx);
        }
    }

    /* Reached end - insert at tail */
    *insert_after = prev;
    return NULL;
}

/**
 * Insert new price level into Q1 at given position
 * Price level slots have volume=0 to distinguish from real orders
 * insert_after: NULL = insert at head, otherwise insert after this node
 */
static OmSlabSlot *insert_price_level_at(OmOrderbookContext *ctx, uint16_t product_id,
                                          uint64_t price, bool is_bid,
                                          OmSlabSlot *insert_after)
{
    OmProductBook *book = &ctx->products[product_id];
    uint32_t *head_idx = is_bid ? &book->bid_head_q1 : &book->ask_head_q1;

    /* Allocate price level slot */
    OmSlabSlot *level = om_slab_alloc(&ctx->slab);
    if (!level) {
        return NULL;
    }

    /* Initialize price level slot
     * volume=0 marks this as a price level, not an actual order
     */
    level->price = price;
    level->volume = 0;
    level->volume_remain = 0;
    level->org = 0;
    level->flags = 0;
    level->order_id = 0;

    /* Initialize all queue nodes to unlinked */
    for (int i = 0; i < OM_MAX_QUEUES; i++) {
        level->queue_nodes[i].next_idx = OM_SLOT_IDX_NULL;
        level->queue_nodes[i].prev_idx = OM_SLOT_IDX_NULL;
    }

    uint32_t level_idx = om_slot_get_idx(&ctx->slab, level);

    /* Insert at the specified position */
    if (insert_after == NULL) {
        /* Insert at head */
        if (*head_idx != OM_SLOT_IDX_NULL) {
            OmSlabSlot *old_head = om_slot_from_idx(&ctx->slab, *head_idx);
            level->queue_nodes[OM_Q1_PRICE_LADDER].next_idx = *head_idx;
            old_head->queue_nodes[OM_Q1_PRICE_LADDER].prev_idx = level_idx;
        }
        *head_idx = level_idx;
    } else {
        /* Insert after the given node */
        om_queue_link_after(&ctx->slab, insert_after, level, OM_Q1_PRICE_LADDER);
    }

    return level;
}

/**
 * Append order to tail of Q2 at given price level
 */
static void append_to_time_queue(OmOrderbookContext *ctx, OmSlabSlot *level,
                                  OmSlabSlot *order)
{
    /* Q2: Time FIFO at this price level
     * level's Q2 is the head (sentinel)
     * We need to find the tail and append there
     */

    /* If level's Q2 is empty, this is first order */
    if (level->queue_nodes[OM_Q2_TIME_FIFO].next_idx == OM_SLOT_IDX_NULL) {
        uint32_t level_idx = om_slot_get_idx(&ctx->slab, level);
        uint32_t order_idx = om_slot_get_idx(&ctx->slab, order);

        level->queue_nodes[OM_Q2_TIME_FIFO].next_idx = order_idx;
        order->queue_nodes[OM_Q2_TIME_FIFO].prev_idx = level_idx;
        order->queue_nodes[OM_Q2_TIME_FIFO].next_idx = OM_SLOT_IDX_NULL;
        return;
    }

    /* Find tail of Q2 (last order at this price) */
    uint32_t curr_idx = level->queue_nodes[OM_Q2_TIME_FIFO].next_idx;
    OmSlabSlot *curr = om_slot_from_idx(&ctx->slab, curr_idx);

    while (curr->queue_nodes[OM_Q2_TIME_FIFO].next_idx != OM_SLOT_IDX_NULL) {
        curr_idx = curr->queue_nodes[OM_Q2_TIME_FIFO].next_idx;
        curr = om_slot_from_idx(&ctx->slab, curr_idx);
    }

    /* Append order after tail */
    om_queue_link_after(&ctx->slab, curr, order, OM_Q2_TIME_FIFO);
}

/**
 * Remove price level from Q1 and free it
 */
static void remove_price_level(OmOrderbookContext *ctx, uint16_t product_id,
                                OmSlabSlot *level, bool is_bid)
{
    OmProductBook *book = &ctx->products[product_id];
    uint32_t *head_idx = is_bid ? &book->bid_head_q1 : &book->ask_head_q1;
    uint32_t level_idx = om_slot_get_idx(&ctx->slab, level);

    /* Update head if removing best level */
    if (*head_idx == level_idx) {
        *head_idx = level->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;
    }

    /* Unlink from Q1 */
    om_queue_unlink(&ctx->slab, level, OM_Q1_PRICE_LADDER);

    /* Free the price level slot */
    om_slab_free(&ctx->slab, level);
}

int om_orderbook_insert(OmOrderbookContext *ctx, uint16_t product_id,
                        OmSlabSlot *order)
{
    uint64_t price = order->price;
    bool is_bid = OM_IS_BID(order->flags);

    /* Find or create price level in Q1 */
    OmSlabSlot *insert_after = NULL;
    OmSlabSlot *level = find_price_level_with_insertion_point(ctx, product_id, price, is_bid, &insert_after);

    if (!level) {
        /* Create new price level at the identified insertion point */
        level = insert_price_level_at(ctx, product_id, price, is_bid, insert_after);
        if (!level) {
            return -1;  /* Out of memory */
        }
    }

    /* Add order to time queue at this price level (Q2) */
    append_to_time_queue(ctx, level, order);

    /* Add order to org queue (Q3) - for now, just link as simple list
     * In production, this would maintain per-org heads
     */
    /* TODO: Implement proper org queue linking when org tracking is added */

    /* Add order to hashmap for O(1) lookup by order_id */
    uint32_t slot_idx = om_slot_get_idx(&ctx->slab, order);
    OmOrderEntry entry = {
        .slot_idx = slot_idx,
        .product_id = product_id
    };
    om_hash_insert(ctx->order_hashmap, order->order_id, entry);

    return 0;
}

bool om_orderbook_cancel(OmOrderbookContext *ctx, uint32_t order_id)
{
    /* Look up order entry from hashmap */
    OmOrderEntry *entry = om_hash_get(ctx->order_hashmap, order_id);
    if (!entry) {
        return false;  /* Order not found in hashmap */
    }

    uint32_t slot_idx = entry->slot_idx;
    uint16_t product_id = entry->product_id;

    OmSlabSlot *order = om_slot_from_idx(&ctx->slab, slot_idx);
    uint64_t price = order->price;
    bool is_bid = OM_IS_BID(order->flags);

    /* Find the price level - for cancel we only need lookup, not insertion point */
    OmSlabSlot *unused = NULL;
    OmSlabSlot *level = find_price_level_with_insertion_point(ctx, product_id, price, is_bid, &unused);
    if (!level) {
        return false;  /* Price level not found */
    }

    /* Remove order from time queue Q2 */
    om_queue_unlink(&ctx->slab, order, OM_Q2_TIME_FIFO);

    /* Check if price level is now empty (no orders in Q2) */
    if (level->queue_nodes[OM_Q2_TIME_FIFO].next_idx == OM_SLOT_IDX_NULL) {
        /* No more orders at this price - remove price level */
        remove_price_level(ctx, product_id, level, is_bid);
    }

    /* Remove from org queue Q3 */
    om_queue_unlink(&ctx->slab, order, OM_Q3_ORG_QUEUE);

    /* Remove from hashmap */
    om_hash_remove(ctx->order_hashmap, order_id);

    /* Free the order slot */
    om_slab_free(&ctx->slab, order);

    return true;
}

uint64_t om_orderbook_get_best_bid(const OmOrderbookContext *ctx, uint16_t product_id)
{
    uint32_t head_idx = ctx->products[product_id].bid_head_q1;
    if (head_idx == OM_SLOT_IDX_NULL) {
        return 0;
    }

    OmSlabSlot *head = om_slot_from_idx((OmDualSlab *)&ctx->slab, head_idx);
    return head->price;
}

uint64_t om_orderbook_get_best_ask(const OmOrderbookContext *ctx, uint16_t product_id)
{
    uint32_t head_idx = ctx->products[product_id].ask_head_q1;
    if (head_idx == OM_SLOT_IDX_NULL) {
        return 0;
    }

    OmSlabSlot *head = om_slot_from_idx((OmDualSlab *)&ctx->slab, head_idx);
    return head->price;
}

uint64_t om_orderbook_get_volume_at_price(const OmOrderbookContext *ctx,
                                           uint16_t product_id, uint64_t price,
                                           bool is_bid)
{
    OmSlabSlot *unused = NULL;
    OmSlabSlot *level = find_price_level_with_insertion_point((OmOrderbookContext *)ctx, product_id, price, is_bid, &unused);
    if (!level) {
        return 0;
    }

    /* Sum volume_remain of all orders in Q2 at this price */
    uint64_t total_volume = 0;
    uint32_t order_idx = level->queue_nodes[OM_Q2_TIME_FIFO].next_idx;

    while (order_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *order = om_slot_from_idx((OmDualSlab *)&ctx->slab, order_idx);
        total_volume += order->volume_remain;
        order_idx = order->queue_nodes[OM_Q2_TIME_FIFO].next_idx;
    }

    return total_volume;
}

OmSlabSlot *om_orderbook_get_slot_by_id(OmOrderbookContext *ctx, uint32_t order_id)
{
    /* Look up order entry from hashmap */
    OmOrderEntry *entry = om_hash_get(ctx->order_hashmap, order_id);
    if (!entry) {
        return NULL;  /* Order not found or not active */
    }

    return om_slot_from_idx(&ctx->slab, entry->slot_idx);
}

bool om_orderbook_price_level_exists(const OmOrderbookContext *ctx,
                                      uint16_t product_id, uint64_t price,
                                      bool is_bid)
{
    OmSlabSlot *unused = NULL;
    OmSlabSlot *level = find_price_level_with_insertion_point((OmOrderbookContext *)ctx, product_id, price, is_bid, &unused);
    return (level != NULL);
}

uint32_t om_orderbook_get_price_level_count(const OmOrderbookContext *ctx,
                                             uint16_t product_id, bool is_bid)
{
    uint32_t head_idx = is_bid ? ctx->products[product_id].bid_head_q1
                               : ctx->products[product_id].ask_head_q1;

    if (head_idx == OM_SLOT_IDX_NULL) {
        return 0;
    }

    uint32_t count = 0;
    uint32_t curr_idx = head_idx;

    while (curr_idx != OM_SLOT_IDX_NULL) {
        count++;
        OmSlabSlot *curr = om_slot_from_idx((OmDualSlab *)&ctx->slab, curr_idx);
        curr_idx = curr->queue_nodes[OM_Q1_PRICE_LADDER].next_idx;
    }

    return count;
}
