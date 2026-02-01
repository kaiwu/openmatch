#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../include/openmatch/om_slab.h"

static inline size_t align_up(size_t size, size_t align) {
    return (size + align - 1) & ~(align - 1);
}

int om_slab_init(OmDualSlab *slab, size_t obj_size) {
    if (!slab || obj_size == 0) {
        return -1;
    }

    memset(slab, 0, sizeof(OmDualSlab));
    slab->obj_size = obj_size;
    slab->split_threshold = OM_SLAB_A_SIZE;

    size_t slot_size = sizeof(OmSlabSlot) + align_up(obj_size, 8);
    slab->slab_a.slot_size = slot_size;
    slab->slab_a.capacity = OM_SLAB_A_SIZE;
    slab->slab_a.used = 0;

    size_t total_a_size = slot_size * OM_SLAB_A_SIZE;
    slab->slab_a.memory = calloc(1, total_a_size);
    if (!slab->slab_a.memory) {
        return -1;
    }

    slab->slab_a.free_list = NULL;
    for (size_t i = 0; i < OM_SLAB_A_SIZE; i++) {
        OmSlabSlot *slot = (OmSlabSlot *)(slab->slab_a.memory + i * slot_size);
        memset(slot->queue_nodes, 0, sizeof(slot->queue_nodes));
        slot->queue_nodes[0].next = slab->slab_a.free_list;
        if (slab->slab_a.free_list) {
            slab->slab_a.free_list->queue_nodes[0].prev = slot;
        }
        slab->slab_a.free_list = slot;
    }

    slab->slab_b.slot_size = slot_size;
    slab->slab_b.slots_per_block = OM_SLAB_B_SIZE;
    slab->slab_b.block_capacity = 4;
    slab->slab_b.block_count = 0;
    slab->slab_b.free_list = NULL;

    slab->slab_b.blocks = calloc(slab->slab_b.block_capacity, sizeof(OmSlabSlot *));
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
    if (!slab_a->free_list) {
        return NULL;
    }

    OmSlabSlot *slot = slab_a->free_list;
    slab_a->free_list = slot->queue_nodes[0].next;
    if (slab_a->free_list) {
        slab_a->free_list->queue_nodes[0].prev = NULL;
    }

    memset(slot->queue_nodes, 0, sizeof(slot->queue_nodes));
    slab_a->used++;
    return slot;
}

static void slab_a_free(OmSlabA *slab_a, OmSlabSlot *slot) {
    if (!slab_a || !slot) return;

    memset(slot->queue_nodes, 0, sizeof(slot->queue_nodes));
    slot->queue_nodes[0].next = slab_a->free_list;
    slot->queue_nodes[0].prev = NULL;
    if (slab_a->free_list) {
        slab_a->free_list->queue_nodes[0].prev = slot;
    }
    slab_a->free_list = slot;
    slab_a->used--;
}

static int slab_b_grow(OmSlabB *slab_b) {
    if (slab_b->block_count >= slab_b->block_capacity) {
        size_t new_capacity = slab_b->block_capacity * 2;
        OmSlabSlot **new_blocks = realloc(slab_b->blocks, new_capacity * sizeof(OmSlabSlot *));
        if (!new_blocks) return -1;
        slab_b->blocks = new_blocks;
        slab_b->block_capacity = new_capacity;
    }

    size_t block_size = slab_b->slot_size * slab_b->slots_per_block;
    OmSlabSlot *block = calloc(1, block_size);
    if (!block) return -1;

    slab_b->blocks[slab_b->block_count++] = block;

    for (size_t i = 0; i < slab_b->slots_per_block; i++) {
        OmSlabSlot *slot = (OmSlabSlot *)((uint8_t *)block + i * slab_b->slot_size);
        slot->queue_nodes[0].next = slab_b->free_list;
        slot->queue_nodes[0].prev = NULL;
        if (slab_b->free_list) {
            slab_b->free_list->queue_nodes[0].prev = slot;
        }
        slab_b->free_list = slot;
    }

    return 0;
}

static OmSlabSlot *slab_b_alloc(OmSlabB *slab_b) {
    if (!slab_b->free_list) {
        if (slab_b_grow(slab_b) != 0) {
            return NULL;
        }
    }

    OmSlabSlot *slot = slab_b->free_list;
    slab_b->free_list = slot->queue_nodes[0].next;
    if (slab_b->free_list) {
        slab_b->free_list->queue_nodes[0].prev = NULL;
    }

    memset(slot->queue_nodes, 0, sizeof(slot->queue_nodes));
    return slot;
}

static void slab_b_free(OmSlabB *slab_b, OmSlabSlot *slot) {
    if (!slab_b || !slot) return;

    memset(slot->queue_nodes, 0, sizeof(slot->queue_nodes));
    slot->queue_nodes[0].next = slab_b->free_list;
    slot->queue_nodes[0].prev = NULL;
    if (slab_b->free_list) {
        slab_b->free_list->queue_nodes[0].prev = slot;
    }
    slab_b->free_list = slot;
}

OmSlabSlot *om_slab_alloc(OmDualSlab *slab) {
    if (!slab) return NULL;

    OmSlabSlot *slot = slab_a_alloc(&slab->slab_a);
    if (slot) return slot;

    return slab_b_alloc(&slab->slab_b);
}

void om_slab_free(OmDualSlab *slab, OmSlabSlot *slot) {
    if (!slab || !slot) return;

    uint8_t *slot_addr = (uint8_t *)slot;
    uint8_t *a_start = slab->slab_a.memory;
    uint8_t *a_end = a_start + (slab->slab_a.slot_size * slab->slab_a.capacity);

    if (slot_addr >= a_start && slot_addr < a_end) {
        slab_a_free(&slab->slab_a, slot);
    } else {
        slab_b_free(&slab->slab_b, slot);
    }
}

void *om_slot_get_data(OmSlabSlot *slot) {
    if (!slot) return NULL;
    return slot->data;
}

int om_queue_init(OmQueue *queue, int queue_id) {
    if (!queue || queue_id < 0 || queue_id >= OM_MAX_QUEUES) {
        return -1;
    }

    queue->head = NULL;
    queue->tail = NULL;
    queue->size = 0;
    queue->queue_id = queue_id;
    return 0;
}

void om_queue_push(OmQueue *queue, OmSlabSlot *slot, int queue_idx) {
    if (!queue || !slot || queue_idx < 0 || queue_idx >= OM_MAX_QUEUES) return;

    OmIntrusiveNode *node = &slot->queue_nodes[queue_idx];

    if (node->next || node->prev || queue->head == slot) {
        return;
    }

    node->next = NULL;
    node->prev = queue->tail;

    if (queue->tail) {
        queue->tail->queue_nodes[queue_idx].next = slot;
    } else {
        queue->head = slot;
    }
    queue->tail = slot;
    queue->size++;
}

OmSlabSlot *om_queue_pop(OmQueue *queue, int queue_idx) {
    if (!queue || queue_idx < 0 || queue_idx >= OM_MAX_QUEUES) return NULL;

    OmSlabSlot *slot = queue->head;
    if (!slot) return NULL;

    OmIntrusiveNode *node = &slot->queue_nodes[queue_idx];
    queue->head = node->next;
    if (queue->head) {
        queue->head->queue_nodes[queue_idx].prev = NULL;
    } else {
        queue->tail = NULL;
    }

    node->next = NULL;
    node->prev = NULL;
    queue->size--;

    return slot;
}

void om_queue_remove(OmQueue *queue, OmSlabSlot *slot, int queue_idx) {
    if (!queue || !slot || queue_idx < 0 || queue_idx >= OM_MAX_QUEUES) return;

    OmIntrusiveNode *node = &slot->queue_nodes[queue_idx];

    if (!node->next && !node->prev && queue->head != slot) {
        return;
    }

    if (node->prev) {
        node->prev->queue_nodes[queue_idx].next = node->next;
    } else {
        queue->head = node->next;
    }

    if (node->next) {
        node->next->queue_nodes[queue_idx].prev = node->prev;
    } else {
        queue->tail = node->prev;
    }

    node->next = NULL;
    node->prev = NULL;
    queue->size--;
}

bool om_queue_is_empty(const OmQueue *queue) {
    if (!queue) return true;
    return queue->size == 0;
}
