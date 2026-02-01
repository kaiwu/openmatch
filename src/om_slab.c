#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../include/openmatch/om_slab.h"

static inline size_t align_up(size_t size, size_t align) {
    return (size + align - 1) & ~(align - 1);
}

/* Helper: Get slot pointer from index in slab A */
static inline OmSlabSlot *idx_to_slot_a(const OmSlabA *slab_a, uint32_t idx) {
    if (idx == OM_SLOT_IDX_NULL || idx >= slab_a->capacity) return NULL;
    return (OmSlabSlot *)(slab_a->memory + idx * slab_a->slot_size);
}

/* Helper: Get index from slot pointer for slab A */
static inline uint32_t slot_to_idx_a(const OmSlabA *slab_a, const OmSlabSlot *slot) {
    if (!slot || !slab_a->memory) return OM_SLOT_IDX_NULL;
    ptrdiff_t offset = (const uint8_t *)slot - slab_a->memory;
    if (offset < 0 || offset % slab_a->slot_size != 0) return OM_SLOT_IDX_NULL;
    return (uint32_t)(offset / slab_a->slot_size);
}

/* Helper: Check if slot is in slab A */
static inline bool slot_in_slab_a(const OmDualSlab *slab, const OmSlabSlot *slot) {
    if (!slab || !slot || !slab->slab_a.memory) return false;
    const uint8_t *slot_addr = (const uint8_t *)slot;
    const uint8_t *a_start = slab->slab_a.memory;
    const uint8_t *a_end = a_start + (slab->slab_a.slot_size * slab->slab_a.capacity);
    return slot_addr >= a_start && slot_addr < a_end;
}

/* Get slot index within slab A (queues only work in slab A) */
uint32_t om_slot_get_idx(const OmDualSlab *slab, const OmSlabSlot *slot) {
    if (!slab || !slot) return OM_SLOT_IDX_NULL;
    return slot_to_idx_a(&slab->slab_a, slot);
}

/* Get slot from index in slab A */
OmSlabSlot *om_slot_from_idx(const OmDualSlab *slab, uint32_t idx) {
    if (!slab || idx == OM_SLOT_IDX_NULL) return NULL;
    return idx_to_slot_a(&slab->slab_a, idx);
}

int om_slab_init(OmDualSlab *slab, size_t user_data_size) {
    if (!slab) {
        return -1;
    }

    memset(slab, 0, sizeof(OmDualSlab));
    slab->user_data_size = user_data_size;
    slab->split_threshold = OM_SLAB_A_SIZE;

    /* Slot size = mandatory fields + queue nodes + user data */
    size_t slot_size = sizeof(OmSlabSlot) + align_up(user_data_size, 8);
    slab->slab_a.slot_size = slot_size;
    slab->slab_a.capacity = OM_SLAB_A_SIZE;
    slab->slab_a.used = 0;

    size_t total_a_size = slot_size * OM_SLAB_A_SIZE;
    slab->slab_a.memory = calloc(1, total_a_size);
    if (!slab->slab_a.memory) {
        return -1;
    }

    /* Build free list using indices (stored in queue_nodes[0]) */
    slab->slab_a.free_list_idx = OM_SLOT_IDX_NULL;
    for (size_t i = 0; i < OM_SLAB_A_SIZE; i++) {
        OmSlabSlot *slot = (OmSlabSlot *)(slab->slab_a.memory + i * slot_size);
        /* Initialize queue nodes to NULL */
        for (int q = 0; q < OM_MAX_QUEUES; q++) {
            slot->queue_nodes[q].next_idx = OM_SLOT_IDX_NULL;
            slot->queue_nodes[q].prev_idx = OM_SLOT_IDX_NULL;
        }
        /* Use queue 0 for free list */
        slot->queue_nodes[0].next_idx = slab->slab_a.free_list_idx;
        slab->slab_a.free_list_idx = (uint32_t)i;
    }

    /* Slab B: User-managed cold data - no mandatory fields, just raw storage */
    slab->slab_b.slot_size = align_up(user_data_size, 8);
    if (slab->slab_b.slot_size == 0) {
        slab->slab_b.slot_size = 8; /* Minimum size */
    }
    slab->slab_b.slots_per_block = OM_SLAB_B_SIZE;
    slab->slab_b.block_capacity = 4;
    slab->slab_b.block_count = 0;
    slab->slab_b.free_list_idx = OM_SLOT_IDX_NULL;

    slab->slab_b.blocks = calloc(slab->slab_b.block_capacity, sizeof(uint8_t *));
    if (!slab->slab_b.blocks) {
        free(slab->slab_a.memory);
        return -1;
    }

    return 0;
}

void om_slab_destroy(OmDualSlab *slab) {
    if (!slab) return;

    free(slab->slab_a.memory);

    for (size_t i = 0; i < slab->slab_b.block_count; i++) {
        free(slab->slab_b.blocks[i]);
    }
    free(slab->slab_b.blocks);

    memset(slab, 0, sizeof(OmDualSlab));
}

static OmSlabSlot *slab_a_alloc(OmSlabA *slab_a) {
    if (slab_a->free_list_idx == OM_SLOT_IDX_NULL) {
        return NULL;
    }

    uint32_t slot_idx = slab_a->free_list_idx;
    OmSlabSlot *slot = idx_to_slot_a(slab_a, slot_idx);
    slab_a->free_list_idx = slot->queue_nodes[0].next_idx;

    /* Clear mandatory fields and queue nodes */
    slot->price = 0;
    slot->volume = 0;
    slot->volume_remain = 0;
    slot->org = 0;
    slot->product = 0;
    slot->flags = 0;
    for (int q = 0; q < OM_MAX_QUEUES; q++) {
        slot->queue_nodes[q].next_idx = OM_SLOT_IDX_NULL;
        slot->queue_nodes[q].prev_idx = OM_SLOT_IDX_NULL;
    }
    slab_a->used++;
    return slot;
}

static void slab_a_free(OmSlabA *slab_a, OmSlabSlot *slot) {
    if (!slab_a || !slot) return;

    uint32_t slot_idx = slot_to_idx_a(slab_a, slot);
    if (slot_idx == OM_SLOT_IDX_NULL) return;

    /* Clear queue nodes and add to free list (using queue 0) */
    for (int q = 0; q < OM_MAX_QUEUES; q++) {
        slot->queue_nodes[q].next_idx = OM_SLOT_IDX_NULL;
        slot->queue_nodes[q].prev_idx = OM_SLOT_IDX_NULL;
    }
    slot->queue_nodes[0].next_idx = slab_a->free_list_idx;
    slab_a->free_list_idx = slot_idx;
    slab_a->used--;
}

static int slab_b_grow(OmSlabB *slab_b) {
    if (slab_b->block_count >= slab_b->block_capacity) {
        size_t new_capacity = slab_b->block_capacity * 2;
        uint8_t **new_blocks = realloc(slab_b->blocks, new_capacity * sizeof(uint8_t *));
        if (!new_blocks) return -1;
        slab_b->blocks = new_blocks;
        slab_b->block_capacity = new_capacity;
    }

    size_t block_size = slab_b->slot_size * slab_b->slots_per_block;
    uint8_t *block = calloc(1, block_size);
    if (!block) return -1;

    uint32_t base_idx = (uint32_t)(slab_b->block_count * slab_b->slots_per_block);
    slab_b->blocks[slab_b->block_count++] = block;

    /* Add all slots to free list (block-major index: block * slots_per_block + slot) */
    for (size_t i = 0; i < slab_b->slots_per_block; i++) {
        uint32_t *next_ptr = (uint32_t *)(block + i * slab_b->slot_size);
        *next_ptr = slab_b->free_list_idx;
        slab_b->free_list_idx = base_idx + (uint32_t)i;
    }

    return 0;
}

static void *slab_b_alloc(OmDualSlab *slab) {
    OmSlabB *slab_b = &slab->slab_b;
    
    if (slab_b->free_list_idx == OM_SLOT_IDX_NULL) {
        if (slab_b_grow(slab_b) != 0) {
            return NULL;
        }
    }

    uint32_t idx = slab_b->free_list_idx;
    size_t block = idx / slab_b->slots_per_block;
    size_t block_idx = idx % slab_b->slots_per_block;
    
    if (block >= slab_b->block_count) return NULL;
    
    uint8_t *slot = slab_b->blocks[block] + block_idx * slab_b->slot_size;
    slab_b->free_list_idx = *(uint32_t *)slot;
    
    /* Clear the slot */
    memset(slot, 0, slab_b->slot_size);
    return slot;
}

static void slab_b_free(OmDualSlab *slab, void *aux_data) {
    if (!slab || !aux_data) return;

    OmSlabB *slab_b = &slab->slab_b;
    
    /* Find which block contains this pointer */
    for (size_t block = 0; block < slab_b->block_count; block++) {
        uint8_t *block_start = slab_b->blocks[block];
        uint8_t *block_end = block_start + (slab_b->slot_size * slab_b->slots_per_block);
        
        if ((uint8_t *)aux_data >= block_start && (uint8_t *)aux_data < block_end) {
            ptrdiff_t offset = (uint8_t *)aux_data - block_start;
            if (offset % slab_b->slot_size != 0) return; /* Not aligned */
            
            uint32_t idx = (uint32_t)(offset / slab_b->slot_size);
            uint32_t global_idx = (uint32_t)(block * slab_b->slots_per_block + idx);
            
            /* Add to free list */
            *(uint32_t *)aux_data = slab_b->free_list_idx;
            slab_b->free_list_idx = global_idx;
            return;
        }
    }
}

OmSlabSlot *om_slab_alloc(OmDualSlab *slab) {
    if (!slab) return NULL;
    return slab_a_alloc(&slab->slab_a);
}

void om_slab_free(OmDualSlab *slab, OmSlabSlot *slot) {
    if (!slab || !slot) return;
    slab_a_free(&slab->slab_a, slot);
}

void *om_slab_alloc_aux(OmDualSlab *slab, OmSlabSlot **parent_slot) {
    if (!slab) return NULL;
    (void)parent_slot; /* Reserved for future use (e.g., linking aux to parent) */
    return slab_b_alloc(slab);
}

void om_slab_free_aux(OmDualSlab *slab, void *aux_data) {
    if (!slab || !aux_data) return;
    slab_b_free(slab, aux_data);
}

void *om_slot_get_data(OmSlabSlot *slot) {
    if (!slot) return NULL;
    return slot->data;
}

int om_queue_init(OmQueue *queue, int queue_id) {
    if (!queue || queue_id < 0 || queue_id >= OM_MAX_QUEUES) {
        return -1;
    }

    queue->head_idx = OM_SLOT_IDX_NULL;
    queue->tail_idx = OM_SLOT_IDX_NULL;
    queue->size = 0;
    queue->queue_id = queue_id;
    return 0;
}

void om_queue_push(OmQueue *queue, OmDualSlab *slab, OmSlabSlot *slot, int queue_idx) {
    if (!queue || !slab || !slot || queue_idx < 0 || queue_idx >= OM_MAX_QUEUES) return;

    OmIntrusiveNode *node = &slot->queue_nodes[queue_idx];

    /* Check if already in queue */
    if (node->next_idx != OM_SLOT_IDX_NULL || node->prev_idx != OM_SLOT_IDX_NULL || 
        queue->head_idx == om_slot_get_idx(slab, slot)) {
        return;
    }

    uint32_t slot_idx = om_slot_get_idx(slab, slot);
    if (slot_idx == OM_SLOT_IDX_NULL) return;

    node->next_idx = OM_SLOT_IDX_NULL;
    node->prev_idx = queue->tail_idx;

    if (queue->tail_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *tail_slot = om_slot_from_idx(slab, queue->tail_idx);
        if (tail_slot) {
            tail_slot->queue_nodes[queue_idx].next_idx = slot_idx;
        }
    } else {
        queue->head_idx = slot_idx;
    }
    queue->tail_idx = slot_idx;
    queue->size++;
}

OmSlabSlot *om_queue_pop(OmQueue *queue, OmDualSlab *slab, int queue_idx) {
    if (!queue || !slab || queue_idx < 0 || queue_idx >= OM_MAX_QUEUES) return NULL;

    if (queue->head_idx == OM_SLOT_IDX_NULL) return NULL;

    OmSlabSlot *slot = om_slot_from_idx(slab, queue->head_idx);
    if (!slot) return NULL;

    OmIntrusiveNode *node = &slot->queue_nodes[queue_idx];
    queue->head_idx = node->next_idx;
    if (queue->head_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *new_head = om_slot_from_idx(slab, queue->head_idx);
        if (new_head) {
            new_head->queue_nodes[queue_idx].prev_idx = OM_SLOT_IDX_NULL;
        }
    } else {
        queue->tail_idx = OM_SLOT_IDX_NULL;
    }

    node->next_idx = OM_SLOT_IDX_NULL;
    node->prev_idx = OM_SLOT_IDX_NULL;
    queue->size--;

    return slot;
}

void om_queue_remove(OmQueue *queue, OmDualSlab *slab, OmSlabSlot *slot, int queue_idx) {
    if (!queue || !slab || !slot || queue_idx < 0 || queue_idx >= OM_MAX_QUEUES) return;

    OmIntrusiveNode *node = &slot->queue_nodes[queue_idx];
    uint32_t slot_idx = om_slot_get_idx(slab, slot);

    /* Check if actually in queue */
    if (node->next_idx == OM_SLOT_IDX_NULL && node->prev_idx == OM_SLOT_IDX_NULL && 
        queue->head_idx != slot_idx) {
        return;
    }

    /* Update previous node */
    if (node->prev_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *prev_slot = om_slot_from_idx(slab, node->prev_idx);
        if (prev_slot) {
            prev_slot->queue_nodes[queue_idx].next_idx = node->next_idx;
        }
    } else {
        queue->head_idx = node->next_idx;
    }

    /* Update next node */
    if (node->next_idx != OM_SLOT_IDX_NULL) {
        OmSlabSlot *next_slot = om_slot_from_idx(slab, node->next_idx);
        if (next_slot) {
            next_slot->queue_nodes[queue_idx].prev_idx = node->prev_idx;
        }
    } else {
        queue->tail_idx = node->prev_idx;
    }

    node->next_idx = OM_SLOT_IDX_NULL;
    node->prev_idx = OM_SLOT_IDX_NULL;
    queue->size--;
}

bool om_queue_is_empty(const OmQueue *queue) {
    if (!queue) return true;
    return queue->size == 0;
}

/* Mandatory field getters */
uint64_t om_slot_get_price(const OmSlabSlot *slot) {
    return slot ? slot->price : 0;
}

uint64_t om_slot_get_volume(const OmSlabSlot *slot) {
    return slot ? slot->volume : 0;
}

uint64_t om_slot_get_volume_remain(const OmSlabSlot *slot) {
    return slot ? slot->volume_remain : 0;
}

uint16_t om_slot_get_org(const OmSlabSlot *slot) {
    return slot ? slot->org : 0;
}

uint16_t om_slot_get_product(const OmSlabSlot *slot) {
    return slot ? slot->product : 0;
}

uint32_t om_slot_get_flags(const OmSlabSlot *slot) {
    return slot ? slot->flags : 0;
}

/* Mandatory field setters */
void om_slot_set_price(OmSlabSlot *slot, uint64_t price) {
    if (slot) slot->price = price;
}

void om_slot_set_volume(OmSlabSlot *slot, uint64_t volume) {
    if (slot) slot->volume = volume;
}

void om_slot_set_volume_remain(OmSlabSlot *slot, uint64_t volume_remain) {
    if (slot) slot->volume_remain = volume_remain;
}

void om_slot_set_org(OmSlabSlot *slot, uint16_t org) {
    if (slot) slot->org = org;
}

void om_slot_set_product(OmSlabSlot *slot, uint16_t product) {
    if (slot) slot->product = product;
}

void om_slot_set_flags(OmSlabSlot *slot, uint32_t flags) {
    if (slot) slot->flags = flags;
}
