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
 * Q1: Price level queue (linking orders at the same price)
 * Q2: Time FIFO queue (time priority within a price level)
 * Q3: Reserved for future use
 */
#define OM_Q0_INTERNAL_FREE 0
#define OM_Q1_PRICE_LEVEL   1
#define OM_Q2_TIME_FIFO     2
#define OM_Q3_RESERVED      3

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
    uint16_t product;        /**< Product ID */
    uint32_t flags;          /**< Order flags (type, side, etc.) */
    
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

typedef struct OmDualSlab {
    OmSlabA slab_a;          /**< Fixed slab with mandatory fields + queues */
    OmSlabB slab_b;          /**< Aux slab for user cold data only */
    size_t split_threshold;
    size_t user_data_size;   /**< Size of user-defined payload per slot */
} OmDualSlab;

int om_slab_init(OmDualSlab *slab, size_t user_data_size);
void om_slab_destroy(OmDualSlab *slab);

OmSlabSlot *om_slab_alloc(OmDualSlab *slab);
void om_slab_free(OmDualSlab *slab, OmSlabSlot *slot);

void *om_slab_alloc_aux(OmDualSlab *slab, OmSlabSlot **parent_slot);
void om_slab_free_aux(OmDualSlab *slab, void *aux_data);

void *om_slot_get_data(OmSlabSlot *slot);

/* Mandatory field accessors */
uint64_t om_slot_get_price(const OmSlabSlot *slot);
uint64_t om_slot_get_volume(const OmSlabSlot *slot);
uint64_t om_slot_get_volume_remain(const OmSlabSlot *slot);
uint16_t om_slot_get_org(const OmSlabSlot *slot);
uint16_t om_slot_get_product(const OmSlabSlot *slot);
uint32_t om_slot_get_flags(const OmSlabSlot *slot);

void om_slot_set_price(OmSlabSlot *slot, uint64_t price);
void om_slot_set_volume(OmSlabSlot *slot, uint64_t volume);
void om_slot_set_volume_remain(OmSlabSlot *slot, uint64_t volume_remain);
void om_slot_set_org(OmSlabSlot *slot, uint16_t org);
void om_slot_set_product(OmSlabSlot *slot, uint16_t product);
void om_slot_set_flags(OmSlabSlot *slot, uint32_t flags);

/* Slot index utilities (only for fixed slab A) */
uint32_t om_slot_get_idx(const OmDualSlab *slab, const OmSlabSlot *slot);
OmSlabSlot *om_slot_from_idx(const OmDualSlab *slab, uint32_t idx);

#endif
