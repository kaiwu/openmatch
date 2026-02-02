#include <check.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include "openmatch/om_engine.h"
#include "openmatch/orderbook.h"
#include "openmatch/om_wal.h"

#define TEST_WAL_FILE "/tmp/test_orderbook.wal"
#define TEST_USER_DATA_SIZE 64
#define TEST_AUX_DATA_SIZE 128

static void cleanup_wal_file(void) {
    unlink(TEST_WAL_FILE);
}

START_TEST(test_wal_basic_write_recovery)
{
    cleanup_wal_file();

    OmSlabConfig config = {
        .user_data_size = TEST_USER_DATA_SIZE,
        .aux_data_size = TEST_AUX_DATA_SIZE,
        .total_slots = 1000
    };

    OmWal wal;
    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 64 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = false,
        .user_data_size = TEST_USER_DATA_SIZE,
        .aux_data_size = TEST_AUX_DATA_SIZE
    };

    ck_assert_int_eq(om_wal_init(&wal, &wal_config), 0);

    OmOrderbookContext ctx;
    ck_assert_int_eq(om_orderbook_init(&ctx, &config, &wal), 0);

    uint32_t order_id = om_slab_next_order_id(&ctx.slab);
    OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(slot);

    om_slot_set_order_id(slot, order_id);
    om_slot_set_price(slot, 10000);
    om_slot_set_volume(slot, 100);
    om_slot_set_volume_remain(slot, 100);
    om_slot_set_flags(slot, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(slot, 42);

    void *user_data = om_slot_get_data(slot);
    ck_assert_ptr_nonnull(user_data);
    memset(user_data, 0xAA, TEST_USER_DATA_SIZE);
    
    void *aux_data = om_slot_get_aux_data(&ctx.slab, slot);
    ck_assert_ptr_nonnull(aux_data);
    memset(aux_data, 0xBB, TEST_AUX_DATA_SIZE);

    ck_assert_int_eq(om_orderbook_insert(&ctx, 0, slot), 0);

    ck_assert_int_eq(om_wal_flush(&wal), 0);
    ck_assert_int_eq(om_wal_fsync(&wal), 0);

    om_wal_close(&wal);
    om_orderbook_destroy(&ctx);

    OmOrderbookContext ctx2;
    ck_assert_int_eq(om_orderbook_init(&ctx2, &config, NULL), 0);

    OmWalReplayStats stats;
    ck_assert_int_eq(om_orderbook_recover_from_wal(&ctx2, TEST_WAL_FILE, &stats), 0);

    ck_assert_uint_eq(stats.records_insert, 1);
    ck_assert_uint_eq(stats.records_cancel, 0);
    ck_assert_uint_eq(stats.records_match, 0);

    OmSlabSlot *recovered = om_orderbook_get_slot_by_id(&ctx2, order_id);
    ck_assert_ptr_nonnull(recovered);

    ck_assert_uint_eq(om_slot_get_order_id(recovered), order_id);
    ck_assert_uint_eq(om_slot_get_price(recovered), 10000);
    ck_assert_uint_eq(om_slot_get_volume(recovered), 100);
    ck_assert_uint_eq(om_slot_get_org(recovered), 42);

    void *recovered_user = om_slot_get_data(recovered);
    ck_assert_ptr_nonnull(recovered_user);
    uint8_t *user_bytes = (uint8_t *)recovered_user;
    for (int i = 0; i < TEST_USER_DATA_SIZE; i++) {
        ck_assert_uint_eq(user_bytes[i], 0xAA);
    }

    void *recovered_aux = om_slot_get_aux_data(&ctx2.slab, recovered);
    ck_assert_ptr_nonnull(recovered_aux);
    uint8_t *aux_bytes = (uint8_t *)recovered_aux;
    for (int i = 0; i < TEST_AUX_DATA_SIZE; i++) {
        ck_assert_uint_eq(aux_bytes[i], 0xBB);
    }

    om_orderbook_destroy(&ctx2);
    cleanup_wal_file();
}
END_TEST

START_TEST(test_wal_sequence_recovery)
{
    cleanup_wal_file();

    OmSlabConfig config = {
        .user_data_size = 32,
        .aux_data_size = 64,
        .total_slots = 1000
    };

    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 64 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = false,
        .user_data_size = 32,
        .aux_data_size = 64
    };

    OmWal wal;
    ck_assert_int_eq(om_wal_init(&wal, &wal_config), 0);
    ck_assert_uint_eq(om_wal_sequence(&wal), 1);

    OmOrderbookContext ctx;
    ck_assert_int_eq(om_orderbook_init(&ctx, &config, &wal), 0);

    for (int i = 0; i < 5; i++) {
        uint32_t order_id = om_slab_next_order_id(&ctx.slab);
        OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
        om_slot_set_order_id(slot, order_id);
        om_slot_set_price(slot, 10000 + i * 100);
        om_slot_set_volume(slot, 100);
        om_slot_set_volume_remain(slot, 100);
        om_slot_set_flags(slot, OM_SIDE_BID | OM_TYPE_LIMIT);
        om_orderbook_insert(&ctx, 0, slot);
    }

    ck_assert_uint_eq(om_wal_sequence(&wal), 6);

    om_wal_flush(&wal);
    om_wal_fsync(&wal);
    om_wal_close(&wal);
    om_orderbook_destroy(&ctx);

    OmWal wal2;
    ck_assert_int_eq(om_wal_init(&wal2, &wal_config), 0);
    ck_assert_uint_eq(om_wal_sequence(&wal2), 6);

    OmOrderbookContext ctx2;
    ck_assert_int_eq(om_orderbook_init(&ctx2, &config, &wal2), 0);

    for (int i = 0; i < 3; i++) {
        uint32_t order_id = om_slab_next_order_id(&ctx2.slab);
        OmSlabSlot *slot = om_slab_alloc(&ctx2.slab);
        om_slot_set_order_id(slot, order_id);
        om_slot_set_price(slot, 20000 + i * 100);
        om_slot_set_volume(slot, 200);
        om_slot_set_volume_remain(slot, 200);
        om_slot_set_flags(slot, OM_SIDE_ASK | OM_TYPE_LIMIT);
        om_orderbook_insert(&ctx2, 0, slot);
    }

    ck_assert_uint_eq(om_wal_sequence(&wal2), 9);

    om_wal_close(&wal2);
    om_orderbook_destroy(&ctx2);
    cleanup_wal_file();
}
END_TEST

START_TEST(test_wal_crc32_validation)
{
    cleanup_wal_file();

    OmSlabConfig config = {
        .user_data_size = 32,
        .aux_data_size = 64,
        .total_slots = 1000
    };

    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 64 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = true,
        .user_data_size = 32,
        .aux_data_size = 64
    };

    OmWal wal;
    ck_assert_int_eq(om_wal_init(&wal, &wal_config), 0);

    OmOrderbookContext ctx;
    ck_assert_int_eq(om_orderbook_init(&ctx, &config, &wal), 0);

    uint32_t order_id = om_slab_next_order_id(&ctx.slab);
    OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
    om_slot_set_order_id(slot, order_id);
    om_slot_set_price(slot, 10000);
    om_slot_set_volume(slot, 100);
    om_slot_set_volume_remain(slot, 100);
    om_slot_set_flags(slot, OM_SIDE_BID | OM_TYPE_LIMIT);
    
    void *user_data = om_slot_get_data(slot);
    memset(user_data, 0xCC, 32);
    void *aux_data = om_slot_get_aux_data(&ctx.slab, slot);
    memset(aux_data, 0xDD, 64);

    ck_assert_int_eq(om_orderbook_insert(&ctx, 0, slot), 0);

    om_wal_flush(&wal);
    om_wal_fsync(&wal);
    om_wal_close(&wal);
    om_orderbook_destroy(&ctx);

    OmWalReplay replay;
    ck_assert_int_eq(om_wal_replay_init_with_config(&replay, TEST_WAL_FILE, &wal_config), 0);

    OmWalType type;
    void *data;
    uint64_t sequence;
    size_t data_len;

    int ret = om_wal_replay_next(&replay, &type, &data, &sequence, &data_len);
    ck_assert_int_eq(ret, 1);
    ck_assert_int_eq(type, OM_WAL_INSERT);
    ck_assert_uint_eq(sequence, 1);

    OmWalInsert *insert = (OmWalInsert *)data;
    ck_assert_uint_eq(insert->order_id, order_id);
    ck_assert_uint_eq(insert->price, 10000);

    ret = om_wal_replay_next(&replay, &type, &data, &sequence, &data_len);
    ck_assert_int_eq(ret, 0);

    om_wal_replay_close(&replay);
    cleanup_wal_file();
}
END_TEST

START_TEST(test_wal_crc32_mismatch)
{
    cleanup_wal_file();

    OmSlabConfig config = {
        .user_data_size = 32,
        .aux_data_size = 64,
        .total_slots = 1000
    };

    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 64 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = true,
        .user_data_size = 32,
        .aux_data_size = 64
    };

    OmWal wal;
    ck_assert_int_eq(om_wal_init(&wal, &wal_config), 0);

    OmOrderbookContext ctx;
    ck_assert_int_eq(om_orderbook_init(&ctx, &config, &wal), 0);

    uint32_t order_id = om_slab_next_order_id(&ctx.slab);
    OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
    om_slot_set_order_id(slot, order_id);
    om_slot_set_price(slot, 10000);
    om_slot_set_volume(slot, 100);
    om_slot_set_volume_remain(slot, 100);
    om_slot_set_flags(slot, OM_SIDE_BID | OM_TYPE_LIMIT);

    void *user_data = om_slot_get_data(slot);
    memset(user_data, 0x11, 32);
    void *aux_data = om_slot_get_aux_data(&ctx.slab, slot);
    memset(aux_data, 0x22, 64);

    ck_assert_int_eq(om_orderbook_insert(&ctx, 0, slot), 0);

    ck_assert_int_eq(om_wal_flush(&wal), 0);
    ck_assert_int_eq(om_wal_fsync(&wal), 0);
    om_wal_close(&wal);
    om_orderbook_destroy(&ctx);

    int fd = open(TEST_WAL_FILE, O_RDWR);
    ck_assert_int_ne(fd, -1);

    off_t offset = (off_t)sizeof(OmWalHeader) + 1;
    ck_assert_int_ne(lseek(fd, offset, SEEK_SET), (off_t)-1);

    uint8_t byte = 0;
    ck_assert_int_eq(read(fd, &byte, sizeof(byte)), (ssize_t)sizeof(byte));
    byte ^= 0xFF;
    ck_assert_int_ne(lseek(fd, offset, SEEK_SET), (off_t)-1);
    ck_assert_int_eq(write(fd, &byte, sizeof(byte)), (ssize_t)sizeof(byte));
    close(fd);

    OmWalReplay replay;
    ck_assert_int_eq(om_wal_replay_init_with_config(&replay, TEST_WAL_FILE, &wal_config), 0);

    OmWalType type;
    void *data;
    uint64_t sequence;
    size_t data_len;

    int ret = om_wal_replay_next(&replay, &type, &data, &sequence, &data_len);
    ck_assert_int_eq(ret, -2);

    om_wal_replay_close(&replay);
    cleanup_wal_file();
}
END_TEST

START_TEST(test_wal_match_replay)
{
    cleanup_wal_file();

    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 64 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = true,
        .user_data_size = 0,
        .aux_data_size = 0
    };

    OmWal wal;
    ck_assert_int_eq(om_wal_init(&wal, &wal_config), 0);

    OmWalMatch match = {
        .maker_id = 101,
        .taker_id = 202,
        .price = 12345,
        .volume = 77,
        .timestamp_ns = 9999,
        .product_id = 5
    };

    ck_assert_uint_ne(om_wal_match(&wal, &match), 0);
    ck_assert_int_eq(om_wal_flush(&wal), 0);
    om_wal_close(&wal);

    OmWalReplay replay;
    ck_assert_int_eq(om_wal_replay_init_with_config(&replay, TEST_WAL_FILE, &wal_config), 0);

    OmWalType type;
    void *data;
    uint64_t sequence;
    size_t data_len;

    int ret = om_wal_replay_next(&replay, &type, &data, &sequence, &data_len);
    ck_assert_int_eq(ret, 1);
    ck_assert_int_eq(type, OM_WAL_MATCH);
    ck_assert_uint_eq(data_len, sizeof(OmWalMatch));

    OmWalMatch *rec = (OmWalMatch *)data;
    ck_assert_uint_eq(rec->maker_id, match.maker_id);
    ck_assert_uint_eq(rec->taker_id, match.taker_id);
    ck_assert_uint_eq(rec->price, match.price);
    ck_assert_uint_eq(rec->volume, match.volume);
    ck_assert_uint_eq(rec->timestamp_ns, match.timestamp_ns);
    ck_assert_uint_eq(rec->product_id, match.product_id);

    ret = om_wal_replay_next(&replay, &type, &data, &sequence, &data_len);
    ck_assert_int_eq(ret, 0);

    om_wal_replay_close(&replay);
    cleanup_wal_file();
}
END_TEST

START_TEST(test_wal_match_recovery_from_engine)
{
    cleanup_wal_file();

    OmSlabConfig slab_config = {
        .user_data_size = 32,
        .aux_data_size = 64,
        .total_slots = 1000
    };

    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 64 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = false,
        .user_data_size = 32,
        .aux_data_size = 64
    };

    OmEngine engine;
    OmEngineConfig engine_config = {
        .slab = slab_config,
        .wal = &wal_config,
        .callbacks = {0}
    };

    ck_assert_int_eq(om_engine_init(&engine, &engine_config), 0);

    OmSlabSlot *maker = om_slab_alloc(&engine.orderbook.slab);
    ck_assert_ptr_nonnull(maker);
    om_slot_set_order_id(maker, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(maker, 10000);
    om_slot_set_volume(maker, 10);
    om_slot_set_volume_remain(maker, 10);
    om_slot_set_flags(maker, OM_SIDE_ASK | OM_TYPE_LIMIT);
    om_slot_set_org(maker, 1);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = om_slab_alloc(&engine.orderbook.slab);
    ck_assert_ptr_nonnull(taker);
    om_slot_set_order_id(taker, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(taker, 10100);
    om_slot_set_volume(taker, 5);
    om_slot_set_volume_remain(taker, 5);
    om_slot_set_flags(taker, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(taker, 1);

    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    om_wal_flush(engine.wal);
    om_wal_fsync(engine.wal);

    uint32_t maker_id = maker->order_id;

    om_engine_destroy(&engine);

    OmOrderbookContext ctx2;
    ck_assert_int_eq(om_orderbook_init(&ctx2, &slab_config, NULL), 0);

    OmWalReplayStats stats;
    ck_assert_int_eq(om_orderbook_recover_from_wal(&ctx2, TEST_WAL_FILE, &stats), 0);

    ck_assert_uint_eq(stats.records_match, 1);

    OmSlabSlot *maker_recovered = om_orderbook_get_slot_by_id(&ctx2, maker_id);
    ck_assert_ptr_nonnull(maker_recovered);
    ck_assert_uint_eq(maker_recovered->volume_remain, 5);

    om_orderbook_destroy(&ctx2);
    cleanup_wal_file();
}
END_TEST

START_TEST(test_wal_aux_data_persistence)
{
    cleanup_wal_file();

    OmSlabConfig config = {
        .user_data_size = 32,
        .aux_data_size = 64,
        .total_slots = 1000
    };

    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 256 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = false,
        .user_data_size = 32,
        .aux_data_size = 64
    };

    OmWal wal;
    ck_assert_int_eq(om_wal_init(&wal, &wal_config), 0);

    OmOrderbookContext ctx;
    ck_assert_int_eq(om_orderbook_init(&ctx, &config, &wal), 0);

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
        om_slot_set_org(slot, (uint16_t)i);

        void *user_data = om_slot_get_data(slot);
        memset(user_data, 0x10 + i, 32);

        void *aux_data = om_slot_get_aux_data(&ctx.slab, slot);
        memset(aux_data, 0x20 + i, 64);

        ck_assert_int_eq(om_orderbook_insert(&ctx, 0, slot), 0);
    }

    om_orderbook_cancel(&ctx, order_ids[3]);
    om_orderbook_cancel(&ctx, order_ids[7]);

    om_wal_flush(&wal);
    om_wal_fsync(&wal);
    om_wal_close(&wal);
    om_orderbook_destroy(&ctx);

    OmOrderbookContext ctx2;
    ck_assert_int_eq(om_orderbook_init(&ctx2, &config, NULL), 0);

    OmWalReplayStats stats;
    ck_assert_int_eq(om_orderbook_recover_from_wal(&ctx2, TEST_WAL_FILE, &stats), 0);

    ck_assert_uint_eq(stats.records_insert, 10);
    ck_assert_uint_eq(stats.records_cancel, 2);

    for (int i = 0; i < 10; i++) {
        OmSlabSlot *slot = om_orderbook_get_slot_by_id(&ctx2, order_ids[i]);
        if (i == 3 || i == 7) {
            ck_assert_ptr_null(slot);
        } else {
            ck_assert_ptr_nonnull(slot);
            ck_assert_uint_eq(om_slot_get_price(slot), 10000 + i * 100);
            
            uint8_t *user = (uint8_t *)om_slot_get_data(slot);
            for (int j = 0; j < 32; j++) {
                ck_assert_uint_eq(user[j], 0x10 + i);
            }

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

START_TEST(test_wal_timestamp_populated)
{
    cleanup_wal_file();

    OmSlabConfig config = {
        .user_data_size = 32,
        .aux_data_size = 64,
        .total_slots = 1000
    };

    OmWalConfig wal_config = {
        .filename = TEST_WAL_FILE,
        .buffer_size = 64 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = false,
        .user_data_size = 32,
        .aux_data_size = 64
    };

    OmWal wal;
    ck_assert_int_eq(om_wal_init(&wal, &wal_config), 0);

    OmOrderbookContext ctx;
    ck_assert_int_eq(om_orderbook_init(&ctx, &config, &wal), 0);

    uint32_t order_id = om_slab_next_order_id(&ctx.slab);
    OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
    om_slot_set_order_id(slot, order_id);
    om_slot_set_price(slot, 10000);
    om_slot_set_volume(slot, 100);
    om_slot_set_volume_remain(slot, 100);
    om_slot_set_flags(slot, OM_SIDE_BID | OM_TYPE_LIMIT);

    ck_assert_int_eq(om_orderbook_insert(&ctx, 0, slot), 0);

    om_wal_flush(&wal);
    om_wal_close(&wal);
    om_orderbook_destroy(&ctx);

    OmWalReplay replay;
    ck_assert_int_eq(om_wal_replay_init_with_config(&replay, TEST_WAL_FILE, &wal_config), 0);

    OmWalType type;
    void *data;
    uint64_t sequence;
    size_t data_len;

    int ret = om_wal_replay_next(&replay, &type, &data, &sequence, &data_len);
    ck_assert_int_eq(ret, 1);
    ck_assert_int_eq(type, OM_WAL_INSERT);

    OmWalInsert *insert = (OmWalInsert *)data;
    ck_assert_uint_ne(insert->timestamp_ns, 0);

    om_wal_replay_close(&replay);
    cleanup_wal_file();
}
END_TEST

Suite *wal_suite(void)
{
    Suite *s = suite_create("WAL");
    TCase *tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_wal_basic_write_recovery);
    tcase_add_test(tc_core, test_wal_sequence_recovery);
    tcase_add_test(tc_core, test_wal_crc32_validation);
    tcase_add_test(tc_core, test_wal_crc32_mismatch);
    tcase_add_test(tc_core, test_wal_aux_data_persistence);
    tcase_add_test(tc_core, test_wal_timestamp_populated);
    tcase_add_test(tc_core, test_wal_match_replay);
    tcase_add_test(tc_core, test_wal_match_recovery_from_engine);

    suite_add_tcase(s, tc_core);
    return s;
}
