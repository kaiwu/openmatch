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

typedef struct OmQueue {
    uint32_t head_idx;  /**< Head slot index (always in fixed slab) */
    uint32_t tail_idx;  /**< Tail slot index (always in fixed slab) */
    size_t size;
    int queue_id;
} OmQueue;

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

int om_queue_init(OmQueue *queue, int queue_id);
void om_queue_push(OmQueue *queue, OmDualSlab *slab, OmSlabSlot *slot, int queue_idx);
OmSlabSlot *om_queue_pop(OmQueue *queue, OmDualSlab *slab, int queue_idx);
void om_queue_remove(OmQueue *queue, OmDualSlab *slab, OmSlabSlot *slot, int queue_idx);
bool om_queue_is_empty(const OmQueue *queue);

#endif
