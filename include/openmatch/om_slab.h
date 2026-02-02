#ifndef OM_SLAB_H
#define OM_SLAB_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#define OM_CACHE_LINE_SIZE 64
#define OM_MAX_QUEUES 4
#define OM_SLAB_A_SIZE 64
#define OM_SLAB_B_SIZE 256
#define OM_SLOT_IDX_NULL UINT32_MAX

/* Order side (1 bit) - bit 0 */
#define OM_SIDE_BID     0x00000000U  /**< Bit 0 = 0: Buy side */
#define OM_SIDE_ASK     0x00000001U  /**< Bit 0 = 1: Sell side */
#define OM_SIDE_MASK    0x00000001U

/* Order type (4 bits) - bits 1-4 */
#define OM_TYPE_LIMIT   0x00000000U  /**< Bits 1-4 = 0: Limit order */
#define OM_TYPE_MARKET  0x00000002U  /**< Bits 1-4 = 1: Market order */
#define OM_TYPE_IOC     0x00000004U  /**< Bits 1-4 = 2: Immediate or Cancel */
#define OM_TYPE_FOK     0x00000006U  /**< Bits 1-4 = 3: Fill or Kill */
#define OM_TYPE_GTC     0x00000008U  /**< Bits 1-4 = 4: Good Till Cancelled */
#define OM_TYPE_MASK    0x0000001EU  /**< Bits 1-4 mask */

/* Order status (3 bits) - bits 5-7 */
#define OM_STATUS_NEW       0x00000000U  /**< Bits 5-7 = 0: New order */
#define OM_STATUS_PARTIAL   0x00000020U  /**< Bits 5-7 = 1: Partially filled */
#define OM_STATUS_FILLED    0x00000040U  /**< Bits 5-7 = 2: Fully filled */
#define OM_STATUS_CANCELLED 0x00000060U  /**< Bits 5-7 = 3: Cancelled */
#define OM_STATUS_REJECTED  0x00000080U  /**< Bits 5-7 = 4: Rejected */
#define OM_STATUS_DEACTIVATED 0x000000A0U  /**< Bits 5-7 = 5: Deactivated */
#define OM_STATUS_MASK      0x000000E0U  /**< Bits 5-7 mask */

/* Bit manipulation helpers */
#define OM_SET_SIDE(flags, side)    (((flags) & ~OM_SIDE_MASK) | ((side) & OM_SIDE_MASK))
#define OM_SET_TYPE(flags, type)    (((flags) & ~OM_TYPE_MASK) | ((type) & OM_TYPE_MASK))
#define OM_SET_STATUS(flags, status) (((flags) & ~OM_STATUS_MASK) | ((status) & OM_STATUS_MASK))

#define OM_GET_SIDE(flags)          ((flags) & OM_SIDE_MASK)
#define OM_GET_TYPE(flags)          ((flags) & OM_TYPE_MASK)
#define OM_GET_STATUS(flags)        ((flags) & OM_STATUS_MASK)

#define OM_IS_BID(flags)            (((flags) & OM_SIDE_MASK) == OM_SIDE_BID)
#define OM_IS_ASK(flags)            (((flags) & OM_SIDE_MASK) == OM_SIDE_ASK)

/* Queue assignment within each slot's queue_nodes[4]:
 * Q0: Internal slab free list (do not use externally)
 * Q1: Price ladder queue (linking different price levels together)
 * Q2: Time FIFO queue (linking orders at the same price by time priority)
 * Q3: Organization queue (linking all orders from same organization across products)
 */
#define OM_Q0_INTERNAL_FREE 0
#define OM_Q1_PRICE_LADDER  1
#define OM_Q2_TIME_FIFO     2
#define OM_Q3_ORG_QUEUE     3

typedef struct OmSlabSlot OmSlabSlot;

typedef struct OmIntrusiveNode {
    uint32_t next_idx;  /**< Next slot index within fixed slab */
    uint32_t prev_idx;  /**< Previous slot index within fixed slab */
} OmIntrusiveNode;

typedef struct OmSlabSlot {
    /* Mandatory fixed fields (32 bytes) */
    uint64_t price;          /**< Order price */
    uint64_t volume;         /**< Original order volume */
    uint64_t volume_remain;  /**< Remaining volume to fill */
    uint16_t org;            /**< Organization ID */
    uint16_t flags;          /**< Order flags (type, side, etc.) - now 16-bit */
    uint32_t order_id;       /**< Unique order ID (persistent across slot reuse) */
    
    /* 4 intrusive queue nodes (32 bytes) - total 64 bytes = one cache line */
    OmIntrusiveNode queue_nodes[OM_MAX_QUEUES];
    
    /* User-defined payload (flexible array member) - starts at cache line 2 */
    uint8_t data[];
} OmSlabSlot;

typedef struct OmSlabA {
    uint32_t free_list_idx;  /**< Index of first free slot in fixed slab */
    uint8_t *memory;
    size_t capacity;
    size_t used;
    size_t slot_size;
} OmSlabA;

typedef struct OmSlabB {
    uint8_t **blocks;        /**< User-managed cold data blocks (no mandatory fields) */
    size_t block_count;
    size_t block_capacity;
    size_t slots_per_block;
    size_t slot_size;
    uint32_t free_list_idx;  /**< Index of first free slot (block-major format) */
} OmSlabB;

/* Slab configuration structure */
typedef struct OmSlabConfig {
    size_t user_data_size;   /**< Size of secondary hot data in fixed slab */
    size_t aux_data_size;    /**< Size of cold data in aux slab */
    uint32_t total_slots;    /**< Total slots in both slabs (must be > 0) */
} OmSlabConfig;

typedef struct OmDualSlab {
    OmSlabA slab_a;          /**< Fixed slab with mandatory fields + queues */
    OmSlabB slab_b;          /**< Aux slab for user cold data only */
    OmSlabConfig config;     /**< Configuration (copied at init) */
    uint32_t next_order_id;  /**< Auto-increment order ID counter (starts at 1) */
} OmDualSlab;

/* Product book structure - array indexed by product_id (0 to 65535)
 * Each product has bid/ask order books using BST for price levels (Q1)
 * Q2 heads are per-price-level (time FIFO), stored within the slot itself
 */
typedef struct OmProductBook {
    /* Q1 price ladder heads - sorted linked list for O(1) best price access
     * Bid: sorted descending (best/highest bid is head)
     * Ask: sorted ascending (best/lowest ask is head)
     * Insertion requires O(N) scan from head, but usually near best price
     */
    uint32_t bid_head_q1;         /**< Head of bid price list (best bid) - O(1) access */
    uint32_t ask_head_q1;         /**< Head of ask price list (best ask) - O(1) access */
} OmProductBook;

#define OM_MAX_PRODUCTS 65536  /**< Maximum number of products (uint16_t max) */

int om_slab_init(OmDualSlab *slab, const OmSlabConfig *config);
void om_slab_destroy(OmDualSlab *slab);

OmSlabSlot *om_slab_alloc(OmDualSlab *slab);
void om_slab_free(OmDualSlab *slab, OmSlabSlot *slot);

/* Generate next unique order ID (auto-increment, starts at 1) */
uint32_t om_slab_next_order_id(OmDualSlab *slab);

/* Slot index utilities (only for fixed slab A) - implemented in .c file */
uint32_t om_slot_get_idx(const OmDualSlab *slab, const OmSlabSlot *slot);
OmSlabSlot *om_slot_from_idx(const OmDualSlab *slab, uint32_t idx);

/* Inline accessor functions for performance */

/* Get pointer to user data (secondary hot data in fixed slab) */
static inline void *om_slot_get_data(OmSlabSlot *slot) {
    return slot->data;
}

/* Get pointer to aux data (cold data in aux slab) - calculated from slot index */
static inline void *om_slot_get_aux_data(OmDualSlab *slab, OmSlabSlot *slot) {
    uint32_t idx = om_slot_get_idx(slab, slot);
    return slab->slab_b.blocks[0] + idx * slab->slab_b.slot_size;
}

/* Mandatory field getters - all inline */
static inline uint64_t om_slot_get_price(const OmSlabSlot *slot) {
    return slot->price;
}

static inline uint64_t om_slot_get_volume(const OmSlabSlot *slot) {
    return slot->volume;
}

static inline uint64_t om_slot_get_volume_remain(const OmSlabSlot *slot) {
    return slot->volume_remain;
}

static inline uint16_t om_slot_get_org(const OmSlabSlot *slot) {
    return slot->org;
}

static inline uint16_t om_slot_get_flags(const OmSlabSlot *slot) {
    return slot->flags;
}

static inline uint32_t om_slot_get_order_id(const OmSlabSlot *slot) {
    return slot->order_id;
}

/* Mandatory field setters - all inline */
static inline void om_slot_set_price(OmSlabSlot *slot, uint64_t price) {
    slot->price = price;
}

static inline void om_slot_set_volume(OmSlabSlot *slot, uint64_t volume) {
    slot->volume = volume;
}

static inline void om_slot_set_volume_remain(OmSlabSlot *slot, uint64_t volume_remain) {
    slot->volume_remain = volume_remain;
}

static inline void om_slot_set_org(OmSlabSlot *slot, uint16_t org) {
    slot->org = org;
}

static inline void om_slot_set_flags(OmSlabSlot *slot, uint16_t flags) {
    slot->flags = flags;
}

static inline void om_slot_set_order_id(OmSlabSlot *slot, uint32_t order_id) {
    slot->order_id = order_id;
}

/* Queue utilities for managing intrusive lists
 * These functions operate on a specific queue index (q_idx) within slots
 * Typical usage: Q1=price ladder, Q2=time FIFO, Q3=org queue
 */

/* Check if slot is linked in a queue */
static inline bool om_queue_is_linked(const OmSlabSlot *slot, int q_idx) {
    return (slot->queue_nodes[q_idx].next_idx != OM_SLOT_IDX_NULL ||
            slot->queue_nodes[q_idx].prev_idx != OM_SLOT_IDX_NULL);
}

/* Link slot after another slot in queue q_idx */
static inline void om_queue_link_after(OmDualSlab *slab, OmSlabSlot *prev, 
                                        OmSlabSlot *slot, int q_idx) {
    uint32_t prev_idx = om_slot_get_idx(slab, prev);
    uint32_t slot_idx = om_slot_get_idx(slab, slot);
    uint32_t next_idx = prev->queue_nodes[q_idx].next_idx;
    
    slot->queue_nodes[q_idx].prev_idx = prev_idx;
    slot->queue_nodes[q_idx].next_idx = next_idx;
    prev->queue_nodes[q_idx].next_idx = slot_idx;
    
    if (next_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *next = om_slot_from_idx(slab, next_idx);
        if (next) next->queue_nodes[q_idx].prev_idx = slot_idx;
    }
}

/* Link slot before another slot in queue q_idx */
static inline void om_queue_link_before(OmDualSlab *slab, OmSlabSlot *next,
                                         OmSlabSlot *slot, int q_idx) {
    uint32_t next_idx = om_slot_get_idx(slab, next);
    uint32_t slot_idx = om_slot_get_idx(slab, slot);
    uint32_t prev_idx = next->queue_nodes[q_idx].prev_idx;
    
    slot->queue_nodes[q_idx].next_idx = next_idx;
    slot->queue_nodes[q_idx].prev_idx = prev_idx;
    next->queue_nodes[q_idx].prev_idx = slot_idx;
    
    if (prev_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *prev = om_slot_from_idx(slab, prev_idx);
        if (prev) prev->queue_nodes[q_idx].next_idx = slot_idx;
    }
}

/* Unlink slot from queue q_idx. Returns true if slot was linked. */
static inline bool om_queue_unlink(OmDualSlab *slab, OmSlabSlot *slot, int q_idx) {
    uint32_t next_idx = slot->queue_nodes[q_idx].next_idx;
    uint32_t prev_idx = slot->queue_nodes[q_idx].prev_idx;
    
    if (next_idx == OM_SLOT_IDX_NULL && prev_idx == OM_SLOT_IDX_NULL) {
        return false;  /* Not linked */
    }
    
    /* Update previous node's next pointer */
    if (prev_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *prev = om_slot_from_idx(slab, prev_idx);
        if (prev) prev->queue_nodes[q_idx].next_idx = next_idx;
    }
    
    /* Update next node's prev pointer */
    if (next_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *next = om_slot_from_idx(slab, next_idx);
        if (next) next->queue_nodes[q_idx].prev_idx = prev_idx;
    }
    
    /* Clear slot's links */
    slot->queue_nodes[q_idx].next_idx = OM_SLOT_IDX_NULL;
    slot->queue_nodes[q_idx].prev_idx = OM_SLOT_IDX_NULL;
    
    return true;
}

#endif
