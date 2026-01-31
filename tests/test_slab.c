#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "openmatch/om_slab.h"

typedef struct TestObject {
    uint64_t id;
    uint32_t value;
    char padding[40];
} TestObject;

static void test_slab_init_destroy(void) {
    printf("Testing slab init/destroy...\n");

    OmDualSlab slab;
    int ret = om_slab_init(&slab, sizeof(TestObject));
    assert(ret == 0);
    assert(slab.obj_size == sizeof(TestObject));

    om_slab_destroy(&slab);
    printf("  PASS\n");
}

static void test_slab_alloc_free(void) {
    printf("Testing slab alloc/free...\n");

    OmDualSlab slab;
    om_slab_init(&slab, sizeof(TestObject));

    OmSlabSlot *slot1 = om_slab_alloc(&slab);
    assert(slot1 != NULL);

    TestObject *obj = (TestObject *)om_slot_get_data(slot1);
    obj->id = 12345;
    obj->value = 42;

    OmSlabSlot *slot2 = om_slab_alloc(&slab);
    assert(slot2 != NULL);
    assert(slot1 != slot2);

    TestObject *obj2 = (TestObject *)om_slot_get_data(slot2);
    obj2->id = 67890;
    obj2->value = 99;

    assert(obj->id == 12345);
    assert(obj->value == 42);

    om_slab_free(&slab, slot1);
    om_slab_free(&slab, slot2);

    om_slab_destroy(&slab);
    printf("  PASS\n");
}

static void test_queue_operations(void) {
    printf("Testing queue operations...\n");

    OmDualSlab slab;
    om_slab_init(&slab, sizeof(TestObject));

    OmQueue queue;
    om_queue_init(&queue, 0);
    assert(om_queue_is_empty(&queue));

    OmSlabSlot *slot1 = om_slab_alloc(&slab);
    OmSlabSlot *slot2 = om_slab_alloc(&slab);
    OmSlabSlot *slot3 = om_slab_alloc(&slab);

    om_queue_push(&queue, slot1, 0);
    assert(!om_queue_is_empty(&queue));
    assert(queue.size == 1);

    om_queue_push(&queue, slot2, 0);
    assert(queue.size == 2);

    om_queue_push(&queue, slot3, 0);
    assert(queue.size == 3);

    OmSlabSlot *popped = om_queue_pop(&queue, 0);
    assert(popped == slot1);
    assert(queue.size == 2);

    om_queue_remove(&queue, slot3, 0);
    assert(queue.size == 1);

    popped = om_queue_pop(&queue, 0);
    assert(popped == slot2);
    assert(queue.size == 0);
    assert(om_queue_is_empty(&queue));

    om_slab_free(&slab, slot1);
    om_slab_free(&slab, slot2);
    om_slab_free(&slab, slot3);

    om_slab_destroy(&slab);
    printf("  PASS\n");
}

static void test_dual_slab(void) {
    printf("Testing dual slab (A + B)...\n");

    OmDualSlab slab;
    om_slab_init(&slab, sizeof(TestObject));

    OmSlabSlot *slots[100];
    for (int i = 0; i < 100; i++) {
        slots[i] = om_slab_alloc(&slab);
        assert(slots[i] != NULL);
        TestObject *obj = (TestObject *)om_slot_get_data(slots[i]);
        obj->id = i;
        obj->value = i * 10;
    }

    for (int i = 0; i < 100; i++) {
        TestObject *obj = (TestObject *)om_slot_get_data(slots[i]);
        assert(obj->id == (uint64_t)i);
        assert(obj->value == (uint32_t)(i * 10));
    }

    for (int i = 0; i < 100; i++) {
        om_slab_free(&slab, slots[i]);
    }

    om_slab_destroy(&slab);
    printf("  PASS\n");
}

static void test_multiple_queues(void) {
    printf("Testing multiple intrusive queues...\n");

    OmDualSlab slab;
    om_slab_init(&slab, sizeof(TestObject));

    OmQueue price_ladder;
    OmQueue time_queue;
    om_queue_init(&price_ladder, 0);
    om_queue_init(&time_queue, 1);

    OmSlabSlot *slot1 = om_slab_alloc(&slab);
    OmSlabSlot *slot2 = om_slab_alloc(&slab);

    om_queue_push(&price_ladder, slot1, 0);
    om_queue_push(&price_ladder, slot2, 0);

    om_queue_push(&time_queue, slot1, 1);
    om_queue_push(&time_queue, slot2, 1);

    assert(price_ladder.size == 2);
    assert(time_queue.size == 2);

    OmSlabSlot *popped_price = om_queue_pop(&price_ladder, 0);
    assert(popped_price == slot1);
    assert(price_ladder.size == 2);
    assert(time_queue.size == 2);

    OmSlabSlot *popped_time = om_queue_pop(&time_queue, 1);
    assert(popped_time == slot1);
    assert(price_ladder.size == 2);
    assert(time_queue.size == 1);

    om_queue_remove(&price_ladder, slot2, 0);
    assert(price_ladder.size == 1);
    assert(time_queue.size == 1);

    om_slab_free(&slab, slot1);
    om_slab_free(&slab, slot2);

    om_slab_destroy(&slab);
    printf("  PASS\n");
}

int main(void) {
    printf("\n=== OpenMatch Slab Allocator Tests ===\n\n");

    test_slab_init_destroy();
    test_slab_alloc_free();
    test_queue_operations();
    test_dual_slab();
    test_multiple_queues();

    printf("\n=== All tests passed! ===\n");
    return 0;
}
