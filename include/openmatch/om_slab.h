#ifndef OM_SLAB_H
#define OM_SLAB_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#define OM_CACHE_LINE_SIZE 64
#define OM_MAX_QUEUES 6
#define OM_SLAB_A_SIZE 64
#define OM_SLAB_B_SIZE 256

typedef struct OmSlabSlot OmSlabSlot;

typedef struct OmIntrusiveNode {
    OmSlabSlot *next;
    OmSlabSlot *prev;
} OmIntrusiveNode;

typedef struct OmSlabSlot {
    OmIntrusiveNode queue_nodes[OM_MAX_QUEUES];
    uint8_t data[];
} OmSlabSlot;

typedef struct OmSlabA {
    OmSlabSlot *free_list;
    uint8_t *memory;
    size_t capacity;
    size_t used;
    size_t slot_size;
} OmSlabA;

typedef struct OmSlabB {
    OmSlabSlot **blocks;
    size_t block_count;
    size_t block_capacity;
    OmSlabSlot *free_list;
    size_t slots_per_block;
    size_t slot_size;
} OmSlabB;

typedef struct OmDualSlab {
    OmSlabA slab_a;
    OmSlabB slab_b;
    size_t split_threshold;
    size_t obj_size;
} OmDualSlab;

typedef struct OmQueue {
    OmSlabSlot *head;
    OmSlabSlot *tail;
    size_t size;
    int queue_id;
} OmQueue;

int om_slab_init(OmDualSlab *slab, size_t obj_size);
void om_slab_destroy(OmDualSlab *slab);

OmSlabSlot *om_slab_alloc(OmDualSlab *slab);
void om_slab_free(OmDualSlab *slab, OmSlabSlot *slot);

void *om_slot_get_data(OmSlabSlot *slot);

int om_queue_init(OmQueue *queue, int queue_id);
void om_queue_push(OmQueue *queue, OmSlabSlot *slot, int queue_idx);
OmSlabSlot *om_queue_pop(OmQueue *queue, int queue_idx);
void om_queue_remove(OmQueue *queue, OmSlabSlot *slot, int queue_idx);
bool om_queue_is_empty(const OmQueue *queue);

#endif
