#include <check.h>
#include <stdlib.h>
#include <string.h>
#include "openmatch/om_slab.h"

START_TEST(test_slab_init)
{
    OmDualSlab slab;
    OmSlabConfig config = {sizeof(uint64_t), sizeof(uint64_t), 64};
    int ret = om_slab_init(&slab, &config);
    ck_assert_int_eq(ret, 0);
    ck_assert_int_eq(slab.config.user_data_size, sizeof(uint64_t));
    
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_slab_init_invalid)
{
    OmDualSlab slab;
    
    // NULL slab pointer
    OmSlabConfig config1 = {sizeof(uint64_t), sizeof(uint64_t), 64};
    int ret = om_slab_init(NULL, &config1);
    ck_assert_int_eq(ret, -1);
    
    // Zero user_data_size is now valid (no user payload)
    OmSlabConfig config2 = {0, 0, 64};
    ret = om_slab_init(&slab, &config2);
    ck_assert_int_eq(ret, 0);
    ck_assert_int_eq(slab.config.user_data_size, 0);
    om_slab_destroy(&slab);
}
END_TEST

START_TEST(test_slab_alloc_free)
{
    OmDualSlab slab;
    OmSlabConfig config = {sizeof(uint64_t), sizeof(uint64_t), 64};
    om_slab_init(&slab, &config);
    
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
    OmSlabConfig config = {sizeof(uint32_t), sizeof(uint32_t), 64};
    om_slab_init(&slab, &config);
    
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

Suite* slab_suite(void)
{
    Suite* s = suite_create("Slab");
    
    TCase* tc_core = tcase_create("Core");
    tcase_add_test(tc_core, test_slab_init);
    tcase_add_test(tc_core, test_slab_init_invalid);
    tcase_add_test(tc_core, test_slab_alloc_free);
    tcase_add_test(tc_core, test_slab_alloc_many);
    suite_add_tcase(s, tc_core);
    
    return s;
}
