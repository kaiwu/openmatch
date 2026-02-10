#include <arpa/inet.h>
#include <check.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>
#include "ombus/om_bus.h"
#include "ombus/om_bus_tcp.h"
#include "ombus/om_bus_wal.h"
#include "ombus/om_bus_market.h"
#include "openmatch/om_engine.h"

/* Unique SHM names per test to avoid collisions */
#define TEST_SHM_PREFIX "/om-bus-test-"

static const char *test_shm_name(const char *suffix) {
    static char buf[128];
    snprintf(buf, sizeof(buf), "%s%s-%d", TEST_SHM_PREFIX, suffix, getpid());
    return buf;
}

/* ---- Test: create / destroy lifecycle ---- */
START_TEST(test_bus_create_destroy) {
    OmBusStream *stream = NULL;
    OmBusStreamConfig cfg = {
        .stream_name = test_shm_name("create"),
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 2,
        .flags = 0,
    };

    int rc = om_bus_stream_create(&stream, &cfg);
    ck_assert_int_eq(rc, 0);
    ck_assert_ptr_nonnull(stream);

    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: single publish + poll roundtrip ---- */
START_TEST(test_bus_publish_poll) {
    const char *name = test_shm_name("pubpoll");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 2,
        .flags = OM_BUS_FLAG_CRC,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = {
        .stream_name = name,
        .consumer_index = 0,
        .zero_copy = false,
    };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Publish a record */
    uint8_t payload[16];
    memset(payload, 0xAB, sizeof(payload));
    ck_assert_int_eq(om_bus_stream_publish(stream, 100, 1, payload, 16), 0);

    /* Poll it back */
    OmBusRecord rec;
    int rc = om_bus_endpoint_poll(ep, &rec);
    ck_assert_int_eq(rc, 1);
    ck_assert_uint_eq(rec.wal_seq, 100);
    ck_assert_uint_eq(rec.wal_type, 1);
    ck_assert_uint_eq(rec.payload_len, 16);
    ck_assert_int_eq(memcmp(rec.payload, payload, 16), 0);

    /* Empty poll */
    rc = om_bus_endpoint_poll(ep, &rec);
    ck_assert_int_eq(rc, 0);

    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: batch publish + poll_batch ---- */
START_TEST(test_bus_batch) {
    const char *name = test_shm_name("batch");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = {
        .stream_name = name,
        .consumer_index = 0,
        .zero_copy = true,
    };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Publish 10 records */
    for (int i = 0; i < 10; i++) {
        uint64_t val = (uint64_t)i;
        ck_assert_int_eq(om_bus_stream_publish(stream, (uint64_t)(i + 1), 1,
                                                &val, sizeof(val)), 0);
    }

    /* Batch poll */
    OmBusRecord recs[16];
    int count = om_bus_endpoint_poll_batch(ep, recs, 16);
    ck_assert_int_eq(count, 10);
    for (int i = 0; i < 10; i++) {
        ck_assert_uint_eq(recs[i].wal_seq, (uint64_t)(i + 1));
        uint64_t val;
        memcpy(&val, recs[i].payload, sizeof(val));
        ck_assert_uint_eq(val, (uint64_t)i);
    }

    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: multi-consumer independence ---- */
START_TEST(test_bus_multi_consumer) {
    const char *name = test_shm_name("multi");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 4,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep0 = NULL, *ep1 = NULL;
    OmBusEndpointConfig ecfg0 = { .stream_name = name, .consumer_index = 0, .zero_copy = true };
    OmBusEndpointConfig ecfg1 = { .stream_name = name, .consumer_index = 1, .zero_copy = true };
    ck_assert_int_eq(om_bus_endpoint_open(&ep0, &ecfg0), 0);
    ck_assert_int_eq(om_bus_endpoint_open(&ep1, &ecfg1), 0);

    /* Publish 5 records */
    for (int i = 0; i < 5; i++) {
        uint64_t val = (uint64_t)(i * 10);
        ck_assert_int_eq(om_bus_stream_publish(stream, (uint64_t)(i + 1), 1,
                                                &val, sizeof(val)), 0);
    }

    /* Consumer 0 reads all 5 */
    OmBusRecord rec;
    for (int i = 0; i < 5; i++) {
        ck_assert_int_ge(om_bus_endpoint_poll(ep0, &rec), 1);
        ck_assert_uint_eq(rec.wal_seq, (uint64_t)(i + 1));
    }
    ck_assert_int_eq(om_bus_endpoint_poll(ep0, &rec), 0);

    /* Consumer 1 independently reads all 5 */
    for (int i = 0; i < 5; i++) {
        ck_assert_int_ge(om_bus_endpoint_poll(ep1, &rec), 1);
        ck_assert_uint_eq(rec.wal_seq, (uint64_t)(i + 1));
    }
    ck_assert_int_eq(om_bus_endpoint_poll(ep1, &rec), 0);

    om_bus_endpoint_close(ep0);
    om_bus_endpoint_close(ep1);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: backpressure (full ring blocks until drain) ---- */
START_TEST(test_bus_backpressure) {
    const char *name = test_shm_name("bp");
    OmBusStream *stream = NULL;
    /* Small ring: capacity=16, we'll use a pattern that fills + drains */
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 16,
        .slot_size = 64,
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    /* Open endpoint BEFORE publishing so its tail tracks from 0 */
    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = { .stream_name = name, .consumer_index = 0, .zero_copy = true };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Fill ring to capacity (16 slots) */
    for (int i = 0; i < 16; i++) {
        uint32_t val = (uint32_t)i;
        ck_assert_int_eq(om_bus_stream_publish(stream, (uint64_t)(i + 1), 1,
                                                &val, sizeof(val)), 0);
    }

    /* Drain 8 records to make room */
    OmBusRecord rec;
    for (int i = 0; i < 8; i++) {
        ck_assert_int_ge(om_bus_endpoint_poll(ep, &rec), 1);
    }

    /* Now we can publish 8 more without blocking */
    for (int i = 16; i < 24; i++) {
        uint32_t val = (uint32_t)i;
        ck_assert_int_eq(om_bus_stream_publish(stream, (uint64_t)(i + 1), 1,
                                                &val, sizeof(val)), 0);
    }

    /* Drain the remaining 16 */
    for (int i = 0; i < 16; i++) {
        ck_assert_int_ge(om_bus_endpoint_poll(ep, &rec), 1);
    }
    ck_assert_int_eq(om_bus_endpoint_poll(ep, &rec), 0);

    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: gap detection (non-contiguous wal_seq) ---- */
START_TEST(test_bus_gap_detection) {
    const char *name = test_shm_name("gap");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = { .stream_name = name, .consumer_index = 0, .zero_copy = true };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Publish wal_seq 1, then skip to 5 */
    uint32_t val = 0;
    ck_assert_int_eq(om_bus_stream_publish(stream, 1, 1, &val, sizeof(val)), 0);
    ck_assert_int_eq(om_bus_stream_publish(stream, 5, 1, &val, sizeof(val)), 0);

    OmBusRecord rec;
    /* First record: wal_seq=1, no gap (first record, expected starts at 0) */
    int rc = om_bus_endpoint_poll(ep, &rec);
    ck_assert_int_eq(rc, 1);
    ck_assert_uint_eq(rec.wal_seq, 1);

    /* Second record: wal_seq=5, expected was 2 -> gap */
    rc = om_bus_endpoint_poll(ep, &rec);
    ck_assert_int_eq(rc, OM_ERR_BUS_GAP_DETECTED);
    ck_assert_uint_eq(rec.wal_seq, 5);

    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: CRC validation (corrupt payload -> error) ---- */
START_TEST(test_bus_crc_validation) {
    const char *name = test_shm_name("crc");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = OM_BUS_FLAG_CRC,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = { .stream_name = name, .consumer_index = 0, .zero_copy = true };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Publish a valid record */
    uint8_t payload[32];
    memset(payload, 0xCC, sizeof(payload));
    ck_assert_int_eq(om_bus_stream_publish(stream, 1, 1, payload, 32), 0);

    /* Corrupt the payload in SHM directly via shm_open + mmap */
    {
        int fd = shm_open(name, O_RDWR, 0);
        ck_assert_int_ge(fd, 0);
        /* Map enough to reach slot 0: header page + consumer_tails + slot */
        size_t map_len = 4096 + 1 * 64 + 256;
        char *m = mmap(NULL, map_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        ck_assert_ptr_ne(m, MAP_FAILED);
        close(fd);
        /* Slot 0 is at: header_page(4096) + max_consumers*64(1*64) + 0*slot_size */
        /* Payload starts at slot + 24 (slot header size) */
        char *p = m + 4096 + 1 * 64 + 24;
        p[0] ^= 0xFF; /* flip a byte */
        munmap(m, map_len);
    }

    OmBusRecord rec;
    int rc = om_bus_endpoint_poll(ep, &rec);
    ck_assert_int_eq(rc, OM_ERR_BUS_CRC_MISMATCH);

    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: record too large -> error ---- */
START_TEST(test_bus_record_too_large) {
    const char *name = test_shm_name("toolarge");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 64, /* payload max = 64 - 24 = 40 bytes */
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    uint8_t payload[48]; /* 48 > 40 */
    memset(payload, 0, sizeof(payload));
    int rc = om_bus_stream_publish(stream, 1, 1, payload, 48);
    ck_assert_int_eq(rc, OM_ERR_BUS_RECORD_TOO_LARGE);

    /* But 40 bytes should work */
    rc = om_bus_stream_publish(stream, 1, 1, payload, 40);
    ck_assert_int_eq(rc, 0);

    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: magic/version mismatch -> endpoint_open error ---- */
START_TEST(test_bus_magic_mismatch) {
    const char *name = test_shm_name("magic");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    /* Corrupt the magic in the SHM header via the stream's internal map.
     * We need to access the map pointer. Since OmBusStream is opaque,
     * we open the SHM directly to corrupt it. */
    {
        int fd = shm_open(name, O_RDWR, 0);
        ck_assert_int_ge(fd, 0);
        uint32_t *magic = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        ck_assert_ptr_ne(magic, MAP_FAILED);
        close(fd);
        *magic = 0xDEADBEEF; /* corrupt magic */
        munmap(magic, 4096);
    }

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = { .stream_name = name, .consumer_index = 0, .zero_copy = true };
    int rc = om_bus_endpoint_open(&ep, &ecfg);
    ck_assert_int_eq(rc, OM_ERR_BUS_MAGIC_MISMATCH);

    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: wal_seq tracking ---- */
START_TEST(test_bus_wal_seq_tracking) {
    const char *name = test_shm_name("walseq");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = { .stream_name = name, .consumer_index = 0, .zero_copy = true };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    ck_assert_uint_eq(om_bus_endpoint_wal_seq(ep), 0);

    uint32_t val = 42;
    ck_assert_int_eq(om_bus_stream_publish(stream, 100, 1, &val, sizeof(val)), 0);
    ck_assert_int_eq(om_bus_stream_publish(stream, 200, 2, &val, sizeof(val)), 0);

    OmBusRecord rec;
    om_bus_endpoint_poll(ep, &rec);
    ck_assert_uint_eq(om_bus_endpoint_wal_seq(ep), 100);

    om_bus_endpoint_poll(ep, &rec);
    ck_assert_uint_eq(om_bus_endpoint_wal_seq(ep), 200);

    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ============================================================================
 * Integration: WAL → Bus → Worker
 * ============================================================================ */

/* Helper: create a temporary WAL file path */
static const char *test_wal_path(const char *suffix) {
    static char buf[256];
    snprintf(buf, sizeof(buf), "/tmp/om-bus-test-%s-%d.wal", suffix, getpid());
    return buf;
}

/* Helper: init engine with WAL and return it */
static void init_test_engine(OmEngine *engine, const char *wal_path) {
    OmWalConfig wal_cfg = {
        .filename = wal_path,
        .buffer_size = 64 * 1024,
        .sync_interval_ms = 0,
        .use_direct_io = false,
        .enable_crc32 = false,
        .user_data_size = 0,
        .aux_data_size = 0,
    };
    OmEngineConfig cfg = {
        .slab = {
            .user_data_size = 0,
            .aux_data_size = 0,
            .total_slots = 256,
        },
        .wal = &wal_cfg,
        .max_products = 4,
        .max_org = 16,
        .hashmap_initial_cap = 0,
        .perf = NULL,
        .callbacks = {0},
    };
    ck_assert_int_eq(om_engine_init(engine, &cfg), 0);
}

/* ---- Test: engine INSERT → bus publish → poll returns correct OmWalInsert ---- */
START_TEST(test_bus_wal_attach) {
    const char *name = test_shm_name("walattach");
    const char *wal_path = test_wal_path("attach");

    /* Create bus stream */
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    /* Create endpoint */
    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = { .stream_name = name, .consumer_index = 0, .zero_copy = false };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Init engine with WAL */
    OmEngine engine;
    init_test_engine(&engine, wal_path);

    /* Attach WAL to bus */
    om_bus_attach_wal(om_engine_get_wal(&engine), stream);

    /* Insert an order → triggers WAL INSERT → post_write → bus publish */
    OmSlabSlot *order = om_slab_alloc(&engine.orderbook.slab);
    ck_assert_ptr_nonnull(order);
    om_slot_set_order_id(order, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(order, 10000);
    om_slot_set_volume(order, 50);
    om_slot_set_volume_remain(order, 50);
    om_slot_set_flags(order, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(order, 1);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, order), 0);

    /* Poll from bus */
    OmBusRecord rec;
    int rc = om_bus_endpoint_poll(ep, &rec);
    ck_assert_int_eq(rc, 1);
    ck_assert_uint_eq(rec.wal_type, OM_WAL_INSERT);
    ck_assert_uint_ge(rec.payload_len, sizeof(OmWalInsert));

    /* Verify INSERT payload */
    const OmWalInsert *ins = (const OmWalInsert *)rec.payload;
    ck_assert_uint_eq(ins->order_id, order->order_id);
    ck_assert_uint_eq(ins->price, 10000);
    ck_assert_uint_eq(ins->volume, 50);
    ck_assert_uint_eq(ins->vol_remain, 50);
    ck_assert_uint_eq(ins->org, 1);
    ck_assert_uint_eq(ins->product_id, 0);

    /* No more records */
    ck_assert_int_eq(om_bus_endpoint_poll(ep, &rec), 0);

    om_bus_endpoint_close(ep);
    om_engine_destroy(&engine);
    om_bus_stream_destroy(stream);
    unlink(wal_path);
}
END_TEST

/* ---- Test: two crossing orders → MATCH + INSERT records arrive on bus ---- */
START_TEST(test_bus_wal_match) {
    const char *name = test_shm_name("walmatch");
    const char *wal_path = test_wal_path("match");

    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = { .stream_name = name, .consumer_index = 0, .zero_copy = false };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    OmEngine engine;
    init_test_engine(&engine, wal_path);
    om_bus_attach_wal(om_engine_get_wal(&engine), stream);

    /* Insert maker (bid at 100) */
    OmSlabSlot *maker = om_slab_alloc(&engine.orderbook.slab);
    ck_assert_ptr_nonnull(maker);
    om_slot_set_order_id(maker, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(maker, 100);
    om_slot_set_volume(maker, 10);
    om_slot_set_volume_remain(maker, 10);
    om_slot_set_flags(maker, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(maker, 1);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    /* Insert taker (ask at 100) → crosses → match */
    OmSlabSlot *taker = om_slab_alloc(&engine.orderbook.slab);
    ck_assert_ptr_nonnull(taker);
    om_slot_set_order_id(taker, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(taker, 100);
    om_slot_set_volume(taker, 10);
    om_slot_set_volume_remain(taker, 10);
    om_slot_set_flags(taker, OM_SIDE_ASK | OM_TYPE_LIMIT);
    om_slot_set_org(taker, 2);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    /* Expect: INSERT (maker) then MATCH, in bus order */
    OmBusRecord rec;
    ck_assert_int_eq(om_bus_endpoint_poll(ep, &rec), 1);
    ck_assert_uint_eq(rec.wal_type, OM_WAL_INSERT);

    ck_assert_int_eq(om_bus_endpoint_poll(ep, &rec), 1);
    ck_assert_uint_eq(rec.wal_type, OM_WAL_MATCH);
    const OmWalMatch *m = (const OmWalMatch *)rec.payload;
    ck_assert_uint_eq(m->maker_id, maker->order_id);
    ck_assert_uint_eq(m->taker_id, taker->order_id);
    ck_assert_uint_eq(m->price, 100);
    ck_assert_uint_eq(m->volume, 10);

    om_bus_endpoint_close(ep);
    om_engine_destroy(&engine);
    om_bus_stream_destroy(stream);
    unlink(wal_path);
}
END_TEST

/* ---- Test: insert + cancel → INSERT + CANCEL records on bus ---- */
START_TEST(test_bus_wal_cancel) {
    const char *name = test_shm_name("walcancel");
    const char *wal_path = test_wal_path("cancel");

    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = { .stream_name = name, .consumer_index = 0, .zero_copy = false };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    OmEngine engine;
    init_test_engine(&engine, wal_path);
    om_bus_attach_wal(om_engine_get_wal(&engine), stream);

    /* Insert an order */
    OmSlabSlot *order = om_slab_alloc(&engine.orderbook.slab);
    ck_assert_ptr_nonnull(order);
    om_slot_set_order_id(order, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(order, 200);
    om_slot_set_volume(order, 25);
    om_slot_set_volume_remain(order, 25);
    om_slot_set_flags(order, OM_SIDE_ASK | OM_TYPE_LIMIT);
    om_slot_set_org(order, 3);
    uint32_t oid = order->order_id;
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 1, order), 0);

    /* Cancel the order */
    ck_assert(om_engine_cancel(&engine, oid));

    /* Bus should have INSERT then CANCEL */
    OmBusRecord rec;
    ck_assert_int_eq(om_bus_endpoint_poll(ep, &rec), 1);
    ck_assert_uint_eq(rec.wal_type, OM_WAL_INSERT);

    ck_assert_int_eq(om_bus_endpoint_poll(ep, &rec), 1);
    ck_assert_uint_eq(rec.wal_type, OM_WAL_CANCEL);
    const OmWalCancel *c = (const OmWalCancel *)rec.payload;
    ck_assert_uint_eq(c->order_id, oid);
    ck_assert_uint_eq(c->product_id, 1);

    om_bus_endpoint_close(ep);
    om_engine_destroy(&engine);
    om_bus_stream_destroy(stream);
    unlink(wal_path);
}
END_TEST

/* Helper dealable: everyone sees everyone's orders fully */
static uint64_t test_bus_dealable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx) {
    (void)ctx;
    if (rec->org == viewer_org) return 0;
    return rec->vol_remain;
}

/* ---- Test: full roundtrip: engine → bus → om_bus_poll_worker → verify qty ---- */
START_TEST(test_bus_worker_roundtrip) {
    const char *name = test_shm_name("walworker");
    const char *wal_path = test_wal_path("worker");

    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name,
        .capacity = 64,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = { .stream_name = name, .consumer_index = 0, .zero_copy = false };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Init engine */
    OmEngine engine;
    init_test_engine(&engine, wal_path);
    om_bus_attach_wal(om_engine_get_wal(&engine), stream);

    /* Init market with org 1 and org 2 subscribing to product 0 */
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) org_to_worker[i] = 0;
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0},
    };
    OmMarketConfig mcfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 16,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 5,
        .dealable = test_bus_dealable,
        .dealable_ctx = NULL,
    };
    ck_assert_int_eq(om_market_init(&market, &mcfg), 0);
    OmMarketWorker *worker = om_market_worker(&market, 0);
    ck_assert_ptr_nonnull(worker);

    /* Insert order (org=1, bid at 500, vol=100) via engine */
    OmSlabSlot *order = om_slab_alloc(&engine.orderbook.slab);
    ck_assert_ptr_nonnull(order);
    om_slot_set_order_id(order, om_slab_next_order_id(&engine.orderbook.slab));
    om_slot_set_price(order, 500);
    om_slot_set_volume(order, 100);
    om_slot_set_volume_remain(order, 100);
    om_slot_set_flags(order, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(order, 1);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, order), 0);

    /* Poll bus → feed worker */
    int rc = om_bus_poll_worker(ep, worker);
    ck_assert_int_eq(rc, 1);

    /* Verify: org 2 should see org 1's order at price 500 with qty 100 */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 500, &qty), 0);
    ck_assert_uint_eq(qty, 100);

    /* org 1 should NOT see its own order (dealable returns 0 for self) */
    ck_assert_int_ne(om_market_worker_get_qty(worker, 1, 0, OM_SIDE_BID, 500, &qty), 0);

    /* No more records */
    ck_assert_int_eq(om_bus_endpoint_poll(ep, (OmBusRecord[1]){{0}}), 0);

    om_bus_endpoint_close(ep);
    om_engine_destroy(&engine);
    om_market_destroy(&market);
    om_bus_stream_destroy(stream);
    unlink(wal_path);
}
END_TEST

/* ============================================================================
 * TCP Transport Tests
 * ============================================================================ */

/* Helper: create server on ephemeral port, return server + port */
static OmBusTcpServer *tcp_test_server(uint32_t max_clients, uint32_t send_buf_size) {
    OmBusTcpServerConfig cfg = {
        .bind_addr = "127.0.0.1",
        .port = 0,
        .max_clients = max_clients ? max_clients : 64,
        .send_buf_size = send_buf_size ? send_buf_size : 256 * 1024,
    };
    OmBusTcpServer *srv = NULL;
    ck_assert_int_eq(om_bus_tcp_server_create(&srv, &cfg), 0);
    ck_assert_ptr_nonnull(srv);
    return srv;
}

static OmBusTcpClient *tcp_test_client(uint16_t port, uint32_t recv_buf_size) {
    OmBusTcpClientConfig cfg = {
        .host = "127.0.0.1",
        .port = port,
        .recv_buf_size = recv_buf_size ? recv_buf_size : 256 * 1024,
    };
    OmBusTcpClient *client = NULL;
    ck_assert_int_eq(om_bus_tcp_client_connect(&client, &cfg), 0);
    ck_assert_ptr_nonnull(client);
    return client;
}

/* ---- Test: server lifecycle ---- */
START_TEST(test_tcp_create_destroy) {
    OmBusTcpServer *srv = tcp_test_server(0, 0);
    ck_assert_uint_gt(om_bus_tcp_server_port(srv), 0);
    ck_assert_uint_eq(om_bus_tcp_server_client_count(srv), 0);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: connect/disconnect ---- */
START_TEST(test_tcp_connect_disconnect) {
    OmBusTcpServer *srv = tcp_test_server(0, 0);
    uint16_t port = om_bus_tcp_server_port(srv);

    OmBusTcpClient *client = tcp_test_client(port, 0);

    /* Server must accept */
    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);
    ck_assert_uint_eq(om_bus_tcp_server_client_count(srv), 1);

    /* Client closes */
    om_bus_tcp_client_close(client);

    /* Server detects disconnect */
    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);
    ck_assert_uint_eq(om_bus_tcp_server_client_count(srv), 0);

    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: single record roundtrip ---- */
START_TEST(test_tcp_single_record) {
    OmBusTcpServer *srv = tcp_test_server(0, 0);
    uint16_t port = om_bus_tcp_server_port(srv);
    OmBusTcpClient *client = tcp_test_client(port, 0);

    /* Accept */
    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);

    /* Broadcast one record */
    uint8_t payload[16];
    memset(payload, 0xAB, sizeof(payload));
    ck_assert_int_eq(om_bus_tcp_server_broadcast(srv, 1, 3, payload, 16), 0);

    /* Flush */
    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);

    /* Small delay for TCP delivery */
    usleep(1000);

    /* Client polls */
    OmBusRecord rec;
    int rc = om_bus_tcp_client_poll(client, &rec);
    ck_assert_int_eq(rc, 1);
    ck_assert_uint_eq(rec.wal_seq, 1);
    ck_assert_uint_eq(rec.wal_type, 3);
    ck_assert_uint_eq(rec.payload_len, 16);
    ck_assert_int_eq(memcmp(rec.payload, payload, 16), 0);

    om_bus_tcp_client_close(client);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: batch broadcast (100 records) ---- */
START_TEST(test_tcp_batch_broadcast) {
    OmBusTcpServer *srv = tcp_test_server(0, 0);
    uint16_t port = om_bus_tcp_server_port(srv);
    OmBusTcpClient *client = tcp_test_client(port, 0);

    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);

    for (int i = 0; i < 100; i++) {
        uint32_t val = (uint32_t)i;
        ck_assert_int_eq(om_bus_tcp_server_broadcast(srv, (uint64_t)(i + 1), 1, &val, sizeof(val)), 0);
    }
    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);

    usleep(5000);

    OmBusRecord rec;
    for (int i = 0; i < 100; i++) {
        int rc;
        /* May need multiple polls if data arrives in chunks */
        do {
            rc = om_bus_tcp_client_poll(client, &rec);
            if (rc == 0) usleep(1000);
        } while (rc == 0);
        ck_assert_int_eq(rc, 1);
        ck_assert_uint_eq(rec.wal_seq, (uint64_t)(i + 1));
        uint32_t val;
        memcpy(&val, rec.payload, sizeof(val));
        ck_assert_uint_eq(val, (uint32_t)i);
    }

    om_bus_tcp_client_close(client);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: slow client (send buffer overflow -> disconnect) ---- */
START_TEST(test_tcp_slow_client) {
    /* Tiny send buffer: 64 bytes — a single 16+16=32 byte frame fits, but not many */
    OmBusTcpServer *srv = tcp_test_server(4, 64);
    uint16_t port = om_bus_tcp_server_port(srv);
    OmBusTcpClient *client = tcp_test_client(port, 0);

    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);
    ck_assert_uint_eq(om_bus_tcp_server_client_count(srv), 1);

    /* Broadcast many records to overflow the tiny send buffer */
    uint8_t payload[32];
    memset(payload, 0, sizeof(payload));
    for (int i = 0; i < 10; i++) {
        om_bus_tcp_server_broadcast(srv, (uint64_t)(i + 1), 1, payload, sizeof(payload));
    }

    /* poll_io should disconnect the overflowed client */
    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);
    ck_assert_uint_eq(om_bus_tcp_server_client_count(srv), 0);

    om_bus_tcp_client_close(client);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: gap detection ---- */
START_TEST(test_tcp_gap_detection) {
    OmBusTcpServer *srv = tcp_test_server(0, 0);
    uint16_t port = om_bus_tcp_server_port(srv);
    OmBusTcpClient *client = tcp_test_client(port, 0);

    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);

    /* Broadcast wal_seq 1, then 5 (gap) */
    uint32_t val = 0;
    ck_assert_int_eq(om_bus_tcp_server_broadcast(srv, 1, 1, &val, sizeof(val)), 0);
    ck_assert_int_eq(om_bus_tcp_server_broadcast(srv, 5, 1, &val, sizeof(val)), 0);
    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);

    usleep(1000);

    OmBusRecord rec;
    /* First record: seq=1, no gap */
    int rc = om_bus_tcp_client_poll(client, &rec);
    ck_assert_int_eq(rc, 1);
    ck_assert_uint_eq(rec.wal_seq, 1);

    /* Second record: seq=5, expected 2 -> gap */
    rc = om_bus_tcp_client_poll(client, &rec);
    ck_assert_int_eq(rc, OM_ERR_BUS_GAP_DETECTED);
    ck_assert_uint_eq(rec.wal_seq, 5);

    om_bus_tcp_client_close(client);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: multi-client ---- */
START_TEST(test_tcp_multi_client) {
    OmBusTcpServer *srv = tcp_test_server(0, 0);
    uint16_t port = om_bus_tcp_server_port(srv);

    OmBusTcpClient *c1 = tcp_test_client(port, 0);
    OmBusTcpClient *c2 = tcp_test_client(port, 0);
    OmBusTcpClient *c3 = tcp_test_client(port, 0);

    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);
    ck_assert_uint_eq(om_bus_tcp_server_client_count(srv), 3);

    /* Broadcast 5 records */
    for (int i = 0; i < 5; i++) {
        uint32_t val = (uint32_t)(i * 10);
        ck_assert_int_eq(om_bus_tcp_server_broadcast(srv, (uint64_t)(i + 1), 1, &val, sizeof(val)), 0);
    }
    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);

    usleep(5000);

    /* Each client reads all 5 */
    OmBusTcpClient *clients[3] = {c1, c2, c3};
    for (int c = 0; c < 3; c++) {
        for (int i = 0; i < 5; i++) {
            OmBusRecord rec;
            int rc;
            do {
                rc = om_bus_tcp_client_poll(clients[c], &rec);
                if (rc == 0) usleep(1000);
            } while (rc == 0);
            ck_assert_int_eq(rc, 1);
            ck_assert_uint_eq(rec.wal_seq, (uint64_t)(i + 1));
        }
    }

    om_bus_tcp_client_close(c1);
    om_bus_tcp_client_close(c2);
    om_bus_tcp_client_close(c3);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: server destroy while connected -> client poll returns DISCONNECTED ---- */
START_TEST(test_tcp_server_destroy_connected) {
    OmBusTcpServer *srv = tcp_test_server(0, 0);
    uint16_t port = om_bus_tcp_server_port(srv);
    OmBusTcpClient *client = tcp_test_client(port, 0);

    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);
    ck_assert_uint_eq(om_bus_tcp_server_client_count(srv), 1);

    /* Destroy server */
    om_bus_tcp_server_destroy(srv);

    usleep(1000);

    /* Client should detect disconnect */
    OmBusRecord rec;
    int rc = om_bus_tcp_client_poll(client, &rec);
    ck_assert_int_eq(rc, OM_ERR_BUS_TCP_DISCONNECTED);

    om_bus_tcp_client_close(client);
}
END_TEST

/* ---- Test: wal_seq tracking ---- */
START_TEST(test_tcp_wal_seq_tracking) {
    OmBusTcpServer *srv = tcp_test_server(0, 0);
    uint16_t port = om_bus_tcp_server_port(srv);
    OmBusTcpClient *client = tcp_test_client(port, 0);

    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);
    ck_assert_uint_eq(om_bus_tcp_client_wal_seq(client), 0);

    uint32_t val = 42;
    ck_assert_int_eq(om_bus_tcp_server_broadcast(srv, 100, 1, &val, sizeof(val)), 0);
    ck_assert_int_eq(om_bus_tcp_server_broadcast(srv, 200, 2, &val, sizeof(val)), 0);
    ck_assert_int_eq(om_bus_tcp_server_poll_io(srv), 0);

    usleep(1000);

    OmBusRecord rec;
    int rc = om_bus_tcp_client_poll(client, &rec);
    ck_assert_int_eq(rc, 1);
    ck_assert_uint_eq(om_bus_tcp_client_wal_seq(client), 100);

    rc = om_bus_tcp_client_poll(client, &rec);
    /* seq jumps 100->200, gap detected since expected=101 */
    ck_assert(rc == 1 || rc == OM_ERR_BUS_GAP_DETECTED);
    ck_assert_uint_eq(om_bus_tcp_client_wal_seq(client), 200);

    om_bus_tcp_client_close(client);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: protocol error (corrupted magic) ---- */
START_TEST(test_tcp_protocol_error) {
    /* Start a mini TCP listener, connect a real client, then send bad frame */
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    ck_assert_int_ge(lfd, 0);
    int reuse = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in laddr;
    memset(&laddr, 0, sizeof(laddr));
    laddr.sin_family = AF_INET;
    laddr.sin_port = 0;
    inet_pton(AF_INET, "127.0.0.1", &laddr.sin_addr);
    ck_assert_int_eq(bind(lfd, (struct sockaddr *)&laddr, sizeof(laddr)), 0);
    ck_assert_int_eq(listen(lfd, 1), 0);
    socklen_t alen = sizeof(laddr);
    getsockname(lfd, (struct sockaddr *)&laddr, &alen);
    uint16_t bad_port = ntohs(laddr.sin_port);

    /* Connect client to mini server */
    OmBusTcpClientConfig bcfg = { .host = "127.0.0.1", .port = bad_port, .recv_buf_size = 4096 };
    OmBusTcpClient *client = NULL;
    ck_assert_int_eq(om_bus_tcp_client_connect(&client, &bcfg), 0);

    /* Accept on server side */
    int afd = accept(lfd, NULL, NULL);
    ck_assert_int_ge(afd, 0);

    /* Send frame with bad magic */
    OmBusTcpFrameHeader bad = {
        .magic = 0xDEADBEEF,
        .wal_type = 1,
        .flags = 0,
        .payload_len = 4,
        .wal_seq = 1,
    };
    uint32_t val = 42;
    write(afd, &bad, sizeof(bad));
    write(afd, &val, sizeof(val));

    usleep(1000);

    OmBusRecord rec;
    int rc = om_bus_tcp_client_poll(client, &rec);
    ck_assert_int_eq(rc, OM_ERR_BUS_TCP_PROTOCOL);

    close(afd);
    close(lfd);
    om_bus_tcp_client_close(client);
}
END_TEST

/* ---- Test: TCP client reconnect + resume ---- */
START_TEST(test_tcp_reconnect_resume) {
    OmBusTcpServer *srv = tcp_test_server(0, 0);
    uint16_t port = om_bus_tcp_server_port(srv);
    OmBusTcpClient *client = tcp_test_client(port, 0);
    om_bus_tcp_server_poll_io(srv);

    /* Broadcast 5 records */
    uint32_t payload = 0;
    for (uint32_t i = 1; i <= 5; i++) {
        payload = i * 100;
        om_bus_tcp_server_broadcast(srv, i, 1, &payload, sizeof(payload));
    }
    om_bus_tcp_server_poll_io(srv);
    usleep(10000);

    OmBusRecord rec;
    for (uint32_t i = 1; i <= 5; i++) {
        ck_assert_int_eq(om_bus_tcp_client_poll(client, &rec), 1);
        ck_assert_uint_eq(rec.wal_seq, i);
    }

    uint64_t last_seq = om_bus_tcp_client_wal_seq(client);
    ck_assert_uint_eq(last_seq, 5);

    /* Disconnect */
    om_bus_tcp_client_close(client);
    om_bus_tcp_server_poll_io(srv);

    /* Reconnect */
    client = tcp_test_client(port, 0);
    om_bus_tcp_server_poll_io(srv);

    /* Broadcast more records */
    for (uint32_t i = 6; i <= 10; i++) {
        payload = i * 100;
        om_bus_tcp_server_broadcast(srv, i, 1, &payload, sizeof(payload));
    }
    om_bus_tcp_server_poll_io(srv);
    usleep(10000);

    for (uint32_t i = 6; i <= 10; i++) {
        ck_assert_int_eq(om_bus_tcp_client_poll(client, &rec), 1);
        ck_assert_uint_eq(rec.wal_seq, i);
    }

    om_bus_tcp_client_close(client);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: TCP max clients exhaustion ---- */
START_TEST(test_tcp_max_clients) {
    OmBusTcpServer *srv = tcp_test_server(3, 0);
    uint16_t port = om_bus_tcp_server_port(srv);

    OmBusTcpClient *c1 = tcp_test_client(port, 0);
    OmBusTcpClient *c2 = tcp_test_client(port, 0);
    OmBusTcpClient *c3 = tcp_test_client(port, 0);
    om_bus_tcp_server_poll_io(srv);
    ck_assert_uint_eq(om_bus_tcp_server_client_count(srv), 3);

    /* 4th client connects at TCP level but server can't accept */
    OmBusTcpClient *c4 = tcp_test_client(port, 0);
    om_bus_tcp_server_poll_io(srv);
    /* Still 3 — extra was closed by server */
    ck_assert_uint_eq(om_bus_tcp_server_client_count(srv), 3);

    /* c4 should get disconnected on poll */
    OmBusRecord rec;
    int rc = om_bus_tcp_client_poll(c4, &rec);
    ck_assert(rc == 0 || rc == OM_ERR_BUS_TCP_DISCONNECTED);

    om_bus_tcp_client_close(c1);
    om_bus_tcp_client_close(c2);
    om_bus_tcp_client_close(c3);
    om_bus_tcp_client_close(c4);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: large payload at SHM slot boundary ---- */
START_TEST(test_bus_large_payload_boundary) {
    const char *name = test_shm_name("boundary");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name, .capacity = 16, .slot_size = 256,
        .max_consumers = 1, .flags = OM_BUS_FLAG_CRC,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = {
        .stream_name = name, .consumer_index = 0, .zero_copy = false,
    };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Max payload = slot_size - 24 (header) = 232 bytes */
    uint8_t payload[232];
    for (int i = 0; i < 232; i++) payload[i] = (uint8_t)(i & 0xFF);

    ck_assert_int_eq(om_bus_stream_publish(stream, 1, 42, payload, 232), 0);

    /* One byte too large should fail */
    ck_assert_int_eq(om_bus_stream_publish(stream, 2, 42, payload, 233),
                     OM_ERR_BUS_RECORD_TOO_LARGE);

    OmBusRecord rec;
    ck_assert_int_eq(om_bus_endpoint_poll(ep, &rec), 1);
    ck_assert_uint_eq(rec.payload_len, 232);
    ck_assert_int_eq(memcmp(rec.payload, payload, 232), 0);

    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: batch publish API ---- */
START_TEST(test_bus_batch_publish) {
    const char *name = test_shm_name("batchpub");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name, .capacity = 64, .slot_size = 256,
        .max_consumers = 1, .flags = OM_BUS_FLAG_CRC,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = {
        .stream_name = name, .consumer_index = 0, .zero_copy = false,
    };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Build batch of 20 records */
    uint32_t payloads[20];
    OmBusRecord recs[20];
    for (int i = 0; i < 20; i++) {
        payloads[i] = (uint32_t)(i * 111);
        recs[i].wal_seq = (uint64_t)(i + 1);
        recs[i].wal_type = 3;
        recs[i].payload_len = sizeof(uint32_t);
        recs[i].payload = &payloads[i];
    }

    ck_assert_int_eq(om_bus_stream_publish_batch(stream, recs, 20), 0);

    /* Poll all 20 */
    OmBusRecord out;
    for (int i = 0; i < 20; i++) {
        ck_assert_int_eq(om_bus_endpoint_poll(ep, &out), 1);
        ck_assert_uint_eq(out.wal_seq, (uint64_t)(i + 1));
        uint32_t val;
        memcpy(&val, out.payload, sizeof(val));
        ck_assert_uint_eq(val, (uint32_t)(i * 111));
    }
    ck_assert_int_eq(om_bus_endpoint_poll(ep, &out), 0);

    /* Verify stats */
    OmBusStreamStats stats;
    om_bus_stream_stats(stream, &stats);
    ck_assert_uint_eq(stats.records_published, 20);

    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: cursor save/load ---- */
START_TEST(test_bus_cursor_persistence) {
    const char *name = test_shm_name("cursor");
    OmBusStream *stream = NULL;
    OmBusStreamConfig scfg = {
        .stream_name = name, .capacity = 64, .slot_size = 256,
        .max_consumers = 1, .flags = 0,
    };
    ck_assert_int_eq(om_bus_stream_create(&stream, &scfg), 0);

    OmBusEndpoint *ep = NULL;
    OmBusEndpointConfig ecfg = {
        .stream_name = name, .consumer_index = 0, .zero_copy = false,
    };
    ck_assert_int_eq(om_bus_endpoint_open(&ep, &ecfg), 0);

    /* Publish and consume 5 records */
    uint32_t payload = 42;
    for (int i = 1; i <= 5; i++) {
        om_bus_stream_publish(stream, (uint64_t)i, 1, &payload, sizeof(payload));
    }
    OmBusRecord rec;
    for (int i = 0; i < 5; i++) {
        ck_assert_int_eq(om_bus_endpoint_poll(ep, &rec), 1);
    }

    /* Save cursor */
    const char *cursor_path = "/tmp/om_bus_test_cursor.bin";
    ck_assert_int_eq(om_bus_endpoint_save_cursor(ep, cursor_path), 0);

    /* Load cursor */
    uint64_t loaded_seq = 0;
    ck_assert_int_eq(om_bus_endpoint_load_cursor(cursor_path, &loaded_seq), 0);
    ck_assert_uint_eq(loaded_seq, 5);

    /* Corrupt file and verify failure */
    int fd = open(cursor_path, O_WRONLY);
    uint8_t garbage = 0xFF;
    pwrite(fd, &garbage, 1, 0);  /* corrupt magic */
    close(fd);
    ck_assert_int_ne(om_bus_endpoint_load_cursor(cursor_path, &loaded_seq), 0);

    unlink(cursor_path);
    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
}
END_TEST

/* ---- Test: TCP server stats ---- */
START_TEST(test_tcp_server_stats) {
    OmBusTcpServer *srv = tcp_test_server(4, 256);
    uint16_t port = om_bus_tcp_server_port(srv);

    OmBusTcpClient *client = tcp_test_client(port, 0);
    om_bus_tcp_server_poll_io(srv);

    /* Broadcast 10 records */
    uint32_t payload = 0xABCD;
    for (int i = 1; i <= 10; i++) {
        om_bus_tcp_server_broadcast(srv, (uint64_t)i, 1, &payload, sizeof(payload));
    }
    om_bus_tcp_server_poll_io(srv);

    OmBusTcpServerStats stats;
    om_bus_tcp_server_stats(srv, &stats);
    ck_assert_uint_eq(stats.records_broadcast, 10);
    ck_assert_uint_eq(stats.bytes_broadcast, 10 * sizeof(uint32_t));
    ck_assert_uint_eq(stats.clients_accepted, 1);

    om_bus_tcp_client_close(client);
    om_bus_tcp_server_poll_io(srv);

    om_bus_tcp_server_stats(srv, &stats);
    ck_assert_uint_eq(stats.clients_disconnected, 1);

    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ---- Test: TCP slow client stats (slow_client_drops) ---- */
START_TEST(test_tcp_slow_client_stats) {
    /* 64-byte send buffer — can only hold ~2-3 frames */
    OmBusTcpServer *srv = tcp_test_server(4, 64);
    uint16_t port = om_bus_tcp_server_port(srv);

    OmBusTcpClient *client = tcp_test_client(port, 0);
    om_bus_tcp_server_poll_io(srv);

    /* Broadcast many records to overflow send buffer */
    uint32_t payload = 42;
    for (int i = 0; i < 100; i++) {
        om_bus_tcp_server_broadcast(srv, (uint64_t)(i + 1), 1, &payload, sizeof(payload));
    }

    OmBusTcpServerStats stats;
    om_bus_tcp_server_stats(srv, &stats);
    ck_assert_uint_gt(stats.slow_client_drops, 0);

    om_bus_tcp_server_poll_io(srv);
    om_bus_tcp_client_close(client);
    om_bus_tcp_server_destroy(srv);
}
END_TEST

/* ============================================================================
 * Suite
 * ============================================================================ */

Suite *bus_suite(void) {
    Suite *s = suite_create("Bus");
    TCase *tc = tcase_create("SHM");
    tcase_add_test(tc, test_bus_create_destroy);
    tcase_add_test(tc, test_bus_publish_poll);
    tcase_add_test(tc, test_bus_batch);
    tcase_add_test(tc, test_bus_multi_consumer);
    tcase_add_test(tc, test_bus_backpressure);
    tcase_add_test(tc, test_bus_gap_detection);
    tcase_add_test(tc, test_bus_crc_validation);
    tcase_add_test(tc, test_bus_record_too_large);
    tcase_add_test(tc, test_bus_magic_mismatch);
    tcase_add_test(tc, test_bus_wal_seq_tracking);
    tcase_add_test(tc, test_bus_large_payload_boundary);
    tcase_add_test(tc, test_bus_batch_publish);
    tcase_add_test(tc, test_bus_cursor_persistence);
    suite_add_tcase(s, tc);

    TCase *tc_wal = tcase_create("WAL-Bus");
    tcase_add_test(tc_wal, test_bus_wal_attach);
    tcase_add_test(tc_wal, test_bus_wal_match);
    tcase_add_test(tc_wal, test_bus_wal_cancel);
    tcase_add_test(tc_wal, test_bus_worker_roundtrip);
    suite_add_tcase(s, tc_wal);

    TCase *tc_tcp = tcase_create("TCP");
    tcase_add_test(tc_tcp, test_tcp_create_destroy);
    tcase_add_test(tc_tcp, test_tcp_connect_disconnect);
    tcase_add_test(tc_tcp, test_tcp_single_record);
    tcase_add_test(tc_tcp, test_tcp_batch_broadcast);
    tcase_add_test(tc_tcp, test_tcp_slow_client);
    tcase_add_test(tc_tcp, test_tcp_gap_detection);
    tcase_add_test(tc_tcp, test_tcp_multi_client);
    tcase_add_test(tc_tcp, test_tcp_server_destroy_connected);
    tcase_add_test(tc_tcp, test_tcp_wal_seq_tracking);
    tcase_add_test(tc_tcp, test_tcp_protocol_error);
    tcase_add_test(tc_tcp, test_tcp_reconnect_resume);
    tcase_add_test(tc_tcp, test_tcp_max_clients);
    tcase_add_test(tc_tcp, test_tcp_server_stats);
    tcase_add_test(tc_tcp, test_tcp_slow_client_stats);
    suite_add_tcase(s, tc_tcp);

    return s;
}
