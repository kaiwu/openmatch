#include <check.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include "ombus/om_bus.h"

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
    suite_add_tcase(s, tc);
    return s;
}
