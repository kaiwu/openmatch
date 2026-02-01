#include <check.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include "openmatch/orderbook.h"
#include "openmatch/om_wal.h"

#define TEST_WAL_FILE "/tmp/test_orderbook.wal"
#define TEST_USER_DATA_SIZE 64
#define TEST_AUX_DATA_SIZE 128

/* Cleanup helper */
static void cleanup_wal_file(void) {
    unlink(TEST_WAL_FILE);
}

/* Test WAL basic write and recovery */
START_TEST(test_wal_basic_write_recovery)
{
    /* Clean up any existing WAL */
    cleanup_wal_file();

    /* Create orderbook with user and aux data */
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = TEST_USER_DATA_SIZE,
        .aux_data_size = TEST_AUX_DATA_SIZE,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config);

    /* Initialize WAL */
    OmWal wal;
    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 64 * 1024,
        .sync_interval_ms = 0,  /* No auto-sync for test */
        .use_direct_io = false, /* Disable O_DIRECT for test compatibility */
        .enable_crc32 = false,
        .user_data_size = TEST_USER_DATA_SIZE,
        .aux_data_size = TEST_AUX_DATA_SIZE
    };

    ck_assert_int_eq(om_wal_init(&wal, &wal_config), 0);

    /* Create and insert an order with data */
    uint32_t order_id = om_slab_next_order_id(&ctx.slab);
    OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(slot);

    /* Set order fields */
    om_slot_set_order_id(slot, order_id);
    om_slot_set_price(slot, 10000);
    om_slot_set_volume(slot, 100);
    om_slot_set_volume_remain(slot, 100);
    om_slot_set_flags(slot, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(slot, 42);

    /* Write user data (secondary hot data) */
    void *user_data = om_slot_get_data(slot);
    ck_assert_ptr_nonnull(user_data);
    memset(user_data, 0xAA, TEST_USER_DATA_SIZE);  /* Fill with pattern */
    
    /* Write aux data (cold data) */
    void *aux_data = om_slot_get_aux_data(&ctx.slab, slot);
    ck_assert_ptr_nonnull(aux_data);
    memset(aux_data, 0xBB, TEST_AUX_DATA_SIZE);  /* Fill with pattern */

    /* Insert into orderbook - this would also log to WAL in production */
    ck_assert_int_eq(om_orderbook_insert(&ctx, 0, slot), 0);

    /* Log to WAL */
    uint64_t seq = om_wal_insert(&wal, slot, 0);
    ck_assert_uint_ne(seq, 0);

    /* Flush WAL to disk */
    ck_assert_int_eq(om_wal_flush(&wal), 0);
    ck_assert_int_eq(om_wal_fsync(&wal), 0);

    /* Close WAL and orderbook */
    om_wal_close(&wal);
    om_orderbook_destroy(&ctx);

    /* Now recover - create fresh orderbook and replay WAL */
    OmOrderbookContext ctx2;
    om_orderbook_init(&ctx2, &config);

    OmWalReplayStats stats;
    ck_assert_int_eq(om_orderbook_recover_from_wal(&ctx2, TEST_WAL_FILE, &stats), 0);

    /* Verify stats */
    ck_assert_uint_eq(stats.records_insert, 1);
    ck_assert_uint_eq(stats.records_cancel, 0);
    ck_assert_uint_eq(stats.records_match, 0);

    /* Verify order was recovered */
    OmSlabSlot *recovered = om_orderbook_get_slot_by_id(&ctx2, order_id);
    ck_assert_ptr_nonnull(recovered);

    /* Verify mandatory fields */
    ck_assert_uint_eq(om_slot_get_order_id(recovered), order_id);
    ck_assert_uint_eq(om_slot_get_price(recovered), 10000);
    ck_assert_uint_eq(om_slot_get_volume(recovered), 100);
    ck_assert_uint_eq(om_slot_get_org(recovered), 42);

    /* Verify user data (secondary hot) was recovered */
    void *recovered_user = om_slot_get_data(recovered);
    ck_assert_ptr_nonnull(recovered_user);
    uint8_t *user_bytes = (uint8_t *)recovered_user;
    for (int i = 0; i < TEST_USER_DATA_SIZE; i++) {
        ck_assert_uint_eq(user_bytes[i], 0xAA);
    }

    /* Verify aux data (cold) was recovered */
    void *recovered_aux = om_slot_get_aux_data(&ctx2.slab, recovered);
    ck_assert_ptr_nonnull(recovered_aux);
    uint8_t *aux_bytes = (uint8_t *)recovered_aux;
    for (int i = 0; i < TEST_AUX_DATA_SIZE; i++) {
        ck_assert_uint_eq(aux_bytes[i], 0xBB);
    }

    /* Cleanup */
    om_orderbook_destroy(&ctx2);
    cleanup_wal_file();
}
END_TEST

/* Test multiple orders with WAL */
START_TEST(test_wal_multiple_orders)
{
    cleanup_wal_file();

    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 32,
        .aux_data_size = 64,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config);

    OmWal wal;
    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 256 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = false,
        .user_data_size = 32,
        .aux_data_size = 64
    };

    ck_assert_int_eq(om_wal_init(&wal, &wal_config), 0);

    /* Insert 10 orders */
    uint32_t order_ids[10];
    for (int i = 0; i < 10; i++) {
        order_ids[i] = om_slab_next_order_id(&ctx.slab);
        OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
        ck_assert_ptr_nonnull(slot);

        om_slot_set_order_id(slot, order_ids[i]);
        om_slot_set_price(slot, 10000 + i * 100);
        om_slot_set_volume(slot, 100);
        om_slot_set_volume_remain(slot, 100);
        om_slot_set_flags(slot, OM_SIDE_BID | OM_TYPE_LIMIT);
        om_slot_set_org(slot, i);

        /* Unique pattern for each order's user data */
        void *user_data = om_slot_get_data(slot);
        memset(user_data, 0x10 + i, 32);

        /* Unique pattern for each order's aux data */
        void *aux_data = om_slot_get_aux_data(&ctx.slab, slot);
        memset(aux_data, 0x20 + i, 64);

        ck_assert_int_eq(om_orderbook_insert(&ctx, 0, slot), 0);
        ck_assert_uint_ne(om_wal_insert(&wal, slot, 0), 0);
    }

    /* Cancel orders 3 and 7 */
    om_orderbook_cancel(&ctx, order_ids[3]);
    om_orderbook_cancel(&ctx, order_ids[7]);

    /* Log cancels to WAL */
    ck_assert_uint_ne(om_wal_cancel(&wal, order_ids[3], 0, 0), 0);
    ck_assert_uint_ne(om_wal_cancel(&wal, order_ids[7], 0, 0), 0);

    om_wal_flush(&wal);
    om_wal_fsync(&wal);
    om_wal_close(&wal);
    om_orderbook_destroy(&ctx);

    /* Recover */
    OmOrderbookContext ctx2;
    om_orderbook_init(&ctx2, &config);

    OmWalReplayStats stats;
    ck_assert_int_eq(om_orderbook_recover_from_wal(&ctx2, TEST_WAL_FILE, &stats), 0);

    /* Verify: 10 inserts, 2 cancels = 8 active orders */
    ck_assert_uint_eq(stats.records_insert, 10);
    ck_assert_uint_eq(stats.records_cancel, 2);

    /* Verify active orders have correct data */
    for (int i = 0; i < 10; i++) {
        OmSlabSlot *slot = om_orderbook_get_slot_by_id(&ctx2, order_ids[i]);
        if (i == 3 || i == 7) {
            /* Cancelled orders should not be found */
            ck_assert_ptr_null(slot);
        } else {
            /* Active orders should have correct data */
            ck_assert_ptr_nonnull(slot);
            ck_assert_uint_eq(om_slot_get_price(slot), 10000 + i * 100);
            
            /* Verify user data pattern */
            uint8_t *user = (uint8_t *)om_slot_get_data(slot);
            for (int j = 0; j < 32; j++) {
                ck_assert_uint_eq(user[j], 0x10 + i);
            }

            /* Verify aux data pattern */
            uint8_t *aux = (uint8_t *)om_slot_get_aux_data(&ctx2.slab, slot);
            for (int j = 0; j < 64; j++) {
                ck_assert_uint_eq(aux[j], 0x20 + i);
            }
        }
    }

    om_orderbook_destroy(&ctx2);
    cleanup_wal_file();
}
END_TEST

Suite *wal_suite(void)
{
    Suite *s = suite_create("WAL");
    TCase *tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_wal_basic_write_recovery);
    tcase_add_test(tc_core, test_wal_multiple_orders);

    suite_add_tcase(s, tc_core);
    return s;
}
