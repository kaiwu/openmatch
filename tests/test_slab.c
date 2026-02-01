#include <check.h>
#include <stdlib.h>
#include <string.h>
#include "openmatch/om_slab.h"

START_TEST(test_slab_init)
{
    OmDualSlab slab;
    int ret = om_slab_init(&slab, sizeof(uint64_t));
    ck_assert_int_eq(ret, 0);
    ck_assert_int_eq(slab.obj_size, sizeof(uint64_t));
    ck_assert_int_eq(slab.split_threshold, OM_SLAB_A_SIZE);
    
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_slab_init_invalid)
{
    OmDualSlab slab;
    
    // NULL slab pointer
    int ret = om_slab_init(NULL, sizeof(uint64_t));
    ck_assert_int_eq(ret, -1);
    
    // Zero object size
    ret = om_slab_init(&slab, 0);
    ck_assert_int_eq(ret, -1);
}
END_TEST

START_TEST(test_slab_alloc_free)
{
    OmDualSlab slab;
    om_slab_init(&slab, sizeof(uint64_t));
    
    // Allocate a slot
    OmSlabSlot *slot = om_slab_alloc(&slab);
    ck_assert_ptr_nonnull(slot);
    
    // Get data pointer
    uint64_t *data = (uint64_t *)om_slot_get_data(slot);
    ck_assert_ptr_nonnull(data);
    
    // Write and read back
    *data = 0xDEADBEEFCAFEBABEULL;
    ck_assert_uint_eq(*data, 0xDEADBEEFCAFEBABEULL);
    
    // Free the slot
    om_slab_free(&slab, slot);
    
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_slab_alloc_many)
{
    OmDualSlab slab;
    om_slab_init(&slab, sizeof(uint32_t));
    
    // Allocate more than slab A capacity
    OmSlabSlot *slots[OM_SLAB_A_SIZE + OM_SLAB_B_SIZE + 10];
    
    for (int i = 0; i < OM_SLAB_A_SIZE + OM_SLAB_B_SIZE + 10; i++) {
        slots[i] = om_slab_alloc(&slab);
        ck_assert_ptr_nonnull(slots[i]);
        
        uint32_t *data = (uint32_t *)om_slot_get_data(slots[i]);
        *data = (uint32_t)i;
    }
    
    // Verify data integrity
    for (int i = 0; i < OM_SLAB_A_SIZE + OM_SLAB_B_SIZE + 10; i++) {
        uint32_t *data = (uint32_t *)om_slot_get_data(slots[i]);
        ck_assert_uint_eq(*data, (uint32_t)i);
    }
    
    // Free all slots
    for (int i = 0; i < OM_SLAB_A_SIZE + OM_SLAB_B_SIZE + 10; i++) {
        om_slab_free(&slab, slots[i]);
    }
    
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_slab_null_handling)
{
    // NULL slab alloc
    OmSlabSlot *slot = om_slab_alloc(NULL);
    ck_assert_ptr_null(slot);
    
    // NULL slot free (should not crash)
    OmDualSlab slab;
    om_slab_init(&slab, sizeof(int));
    om_slab_free(&slab, NULL);
    
    // NULL slot data
    void *data = om_slot_get_data(NULL);
    ck_assert_ptr_null(data);
    
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_queue_init)
{
    OmQueue queue;
    int ret = om_queue_init(&queue, 0);
    ck_assert_int_eq(ret, 0);
    ck_assert_ptr_null(queue.head);
    ck_assert_ptr_null(queue.tail);
    ck_assert_uint_eq(queue.size, 0);
    ck_assert_int_eq(queue.queue_id, 0);
}
END_TEST

START_TEST(test_queue_init_invalid)
{
    OmQueue queue;
    
    // NULL queue
    int ret = om_queue_init(NULL, 0);
    ck_assert_int_eq(ret, -1);
    
    // Invalid queue_id (negative)
    ret = om_queue_init(&queue, -1);
    ck_assert_int_eq(ret, -1);
    
    // Invalid queue_id (>= OM_MAX_QUEUES)
    ret = om_queue_init(&queue, OM_MAX_QUEUES);
    ck_assert_int_eq(ret, -1);
}
END_TEST

START_TEST(test_queue_push_pop)
{
    OmDualSlab slab;
    om_slab_init(&slab, sizeof(uint64_t));
    
    OmQueue queue;
    om_queue_init(&queue, 0);
    
    // Allocate and push slots
    OmSlabSlot *slot1 = om_slab_alloc(&slab);
    OmSlabSlot *slot2 = om_slab_alloc(&slab);
    OmSlabSlot *slot3 = om_slab_alloc(&slab);
    
    om_queue_push(&queue, slot1, 0);
    ck_assert_uint_eq(queue.size, 1);
    ck_assert_ptr_eq(queue.head, slot1);
    ck_assert_ptr_eq(queue.tail, slot1);
    
    om_queue_push(&queue, slot2, 0);
    ck_assert_uint_eq(queue.size, 2);
    ck_assert_ptr_eq(queue.head, slot1);
    ck_assert_ptr_eq(queue.tail, slot2);
    
    om_queue_push(&queue, slot3, 0);
    ck_assert_uint_eq(queue.size, 3);
    
    // Pop slots
    OmSlabSlot *popped1 = om_queue_pop(&queue, 0);
    ck_assert_ptr_eq(popped1, slot1);
    ck_assert_uint_eq(queue.size, 2);
    
    OmSlabSlot *popped2 = om_queue_pop(&queue, 0);
    ck_assert_ptr_eq(popped2, slot2);
    ck_assert_uint_eq(queue.size, 1);
    
    OmSlabSlot *popped3 = om_queue_pop(&queue, 0);
    ck_assert_ptr_eq(popped3, slot3);
    ck_assert_uint_eq(queue.size, 0);
    ck_assert_ptr_null(queue.head);
    ck_assert_ptr_null(queue.tail);
    
    // Pop from empty queue
    OmSlabSlot *empty_pop = om_queue_pop(&queue, 0);
    ck_assert_ptr_null(empty_pop);
    
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_queue_remove)
{
    OmDualSlab slab;
    om_slab_init(&slab, sizeof(uint64_t));
    
    OmQueue queue;
    om_queue_init(&queue, 0);
    
    OmSlabSlot *slot1 = om_slab_alloc(&slab);
    OmSlabSlot *slot2 = om_slab_alloc(&slab);
    OmSlabSlot *slot3 = om_slab_alloc(&slab);
    
    om_queue_push(&queue, slot1, 0);
    om_queue_push(&queue, slot2, 0);
    om_queue_push(&queue, slot3, 0);
    ck_assert_uint_eq(queue.size, 3);
    
    // Remove from middle
    om_queue_remove(&queue, slot2, 0);
    ck_assert_uint_eq(queue.size, 2);
    
    // Verify order
    OmSlabSlot *popped1 = om_queue_pop(&queue, 0);
    ck_assert_ptr_eq(popped1, slot1);
    OmSlabSlot *popped3 = om_queue_pop(&queue, 0);
    ck_assert_ptr_eq(popped3, slot3);
    
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_queue_is_empty)
{
    OmDualSlab slab;
    om_slab_init(&slab, sizeof(uint64_t));
    
    OmQueue queue;
    om_queue_init(&queue, 0);
    
    // Empty queue
    ck_assert(om_queue_is_empty(&queue));
    
    // Add element
    OmSlabSlot *slot = om_slab_alloc(&slab);
    om_queue_push(&queue, slot, 0);
    ck_assert(!om_queue_is_empty(&queue));
    
    // Remove element
    om_queue_pop(&queue, 0);
    ck_assert(om_queue_is_empty(&queue));
    
    // NULL queue
    ck_assert(om_queue_is_empty(NULL));
    
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_multiple_queues)
{
    OmDualSlab slab;
    om_slab_init(&slab, sizeof(uint64_t));
    
    // Create multiple queues on same slots
    OmQueue queue0, queue1;
    om_queue_init(&queue0, 0);
    om_queue_init(&queue1, 1);
    
    OmSlabSlot *slot = om_slab_alloc(&slab);
    
    // Push to both queues
    om_queue_push(&queue0, slot, 0);
    om_queue_push(&queue1, slot, 1);
    
    ck_assert_uint_eq(queue0.size, 1);
    ck_assert_uint_eq(queue1.size, 1);
    
    // Pop from queue0
    OmSlabSlot *popped0 = om_queue_pop(&queue0, 0);
    ck_assert_ptr_eq(popped0, slot);
    ck_assert(om_queue_is_empty(&queue0));
    
    // slot should still be in queue1
    ck_assert(!om_queue_is_empty(&queue1));
    OmSlabSlot *popped1 = om_queue_pop(&queue1, 1);
    ck_assert_ptr_eq(popped1, slot);
    
    om_slab_destroy(&slab);
}
END_TEST

Suite* slab_suite(void)
{
    Suite* s = suite_create("Slab");
    
    TCase* tc_core = tcase_create("Core");
    tcase_add_test(tc_core, test_slab_init);
    tcase_add_test(tc_core, test_slab_init_invalid);
    tcase_add_test(tc_core, test_slab_alloc_free);
    tcase_add_test(tc_core, test_slab_alloc_many);
    tcase_add_test(tc_core, test_slab_null_handling);
    suite_add_tcase(s, tc_core);
    
    TCase* tc_queue = tcase_create("Queue");
    tcase_add_test(tc_queue, test_queue_init);
    tcase_add_test(tc_queue, test_queue_init_invalid);
    tcase_add_test(tc_queue, test_queue_push_pop);
    tcase_add_test(tc_queue, test_queue_remove);
    tcase_add_test(tc_queue, test_queue_is_empty);
    tcase_add_test(tc_queue, test_multiple_queues);
    suite_add_tcase(s, tc_queue);
    
    return s;
}
