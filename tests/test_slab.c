#include <check.h>
#include <stdlib.h>
#include <string.h>
#include "openmatch/om_slab.h"

START_TEST(test_slab_init)
{
    OmDualSlab slab;
    int ret = om_slab_init(&slab, sizeof(uint64_t));
    ck_assert_int_eq(ret, 0);
    ck_assert_int_eq(slab.user_data_size, sizeof(uint64_t));
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
    
    // Zero user_data_size is now valid (no user payload)
    ret = om_slab_init(&slab, 0);
    ck_assert_int_eq(ret, 0);
    ck_assert_int_eq(slab.user_data_size, 0);
    om_slab_destroy(&slab);
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
    
    // Allocate all fixed slab slots (slab B is for aux data only)
    OmSlabSlot *slots[OM_SLAB_A_SIZE];
    
    for (int i = 0; i < OM_SLAB_A_SIZE; i++) {
        slots[i] = om_slab_alloc(&slab);
        ck_assert_ptr_nonnull(slots[i]);
        
        uint32_t *data = (uint32_t *)om_slot_get_data(slots[i]);
        *data = (uint32_t)i;
    }
    
    // Next allocation should fail (no more slots in fixed slab)
    OmSlabSlot *extra = om_slab_alloc(&slab);
    ck_assert_ptr_null(extra);
    
    // Verify data integrity
    for (int i = 0; i < OM_SLAB_A_SIZE; i++) {
        uint32_t *data = (uint32_t *)om_slot_get_data(slots[i]);
        ck_assert_uint_eq(*data, (uint32_t)i);
    }
    
    // Free all slots
    for (int i = 0; i < OM_SLAB_A_SIZE; i++) {
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

START_TEST(test_mandatory_fields)
{
    OmDualSlab slab;
    om_slab_init(&slab, sizeof(uint64_t));
    
    OmSlabSlot *slot = om_slab_alloc(&slab);
    ck_assert_ptr_nonnull(slot);
    
    // Check initial values are zero
    ck_assert_uint_eq(om_slot_get_price(slot), 0);
    ck_assert_uint_eq(om_slot_get_volume(slot), 0);
    ck_assert_uint_eq(om_slot_get_volume_remain(slot), 0);
    ck_assert_uint_eq(om_slot_get_org(slot), 0);
    ck_assert_uint_eq(om_slot_get_flags(slot), 0);
    
    // Set values
    om_slot_set_price(slot, 12345);
    om_slot_set_volume(slot, 1000);
    om_slot_set_volume_remain(slot, 500);
    om_slot_set_org(slot, 42);
    om_slot_set_flags(slot, 0xBEEF);
    
    // Verify values
    ck_assert_uint_eq(om_slot_get_price(slot), 12345);
    ck_assert_uint_eq(om_slot_get_volume(slot), 1000);
    ck_assert_uint_eq(om_slot_get_volume_remain(slot), 500);
    ck_assert_uint_eq(om_slot_get_org(slot), 42);
    ck_assert_uint_eq(om_slot_get_flags(slot), 0xBEEF);
    
    om_slab_free(&slab, slot);
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_alloc_clears_mandatory_fields)
{
    OmDualSlab slab;
    om_slab_init(&slab, 0);  // No user data
    
    OmSlabSlot *slot = om_slab_alloc(&slab);
    ck_assert_ptr_nonnull(slot);
    
    // Set values
    om_slot_set_price(slot, 99999);
    om_slot_set_volume(slot, 88888);
    
    // Free and reallocate
    om_slab_free(&slab, slot);
    slot = om_slab_alloc(&slab);
    
    // Values should be cleared
    ck_assert_uint_eq(om_slot_get_price(slot), 0);
    ck_assert_uint_eq(om_slot_get_volume(slot), 0);
    
    om_slab_free(&slab, slot);
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
    
    TCase* tc_fields = tcase_create("MandatoryFields");
    tcase_add_test(tc_fields, test_mandatory_fields);
    tcase_add_test(tc_fields, test_alloc_clears_mandatory_fields);
    suite_add_tcase(s, tc_fields);
    
    return s;
}
