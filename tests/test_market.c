#include <check.h>
#include <pthread.h>
#include <stdint.h>
#include "openmarket/om_worker.h"
#include "openmarket/om_market.h"

START_TEST(test_market_ring_basic) {
    OmMarketRing ring;
    OmMarketRingConfig config = {
        .capacity = 8,
        .consumer_count = 2
    };

    ck_assert_int_eq(om_market_ring_init(&ring, &config), 0);
    ck_assert_int_eq(om_market_ring_register_consumer(&ring, 0), 0);
    ck_assert_int_eq(om_market_ring_register_consumer(&ring, 1), 0);

    uint64_t a = 1;
    uint64_t b = 2;
    void *out = NULL;

    ck_assert_int_eq(om_market_ring_enqueue(&ring, &a), 0);
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &b), 0);

    ck_assert_int_eq(om_market_ring_dequeue(&ring, 0, &out), 1);
    ck_assert_ptr_eq(out, &a);
    ck_assert_int_eq(om_market_ring_dequeue(&ring, 1, &out), 1);
    ck_assert_ptr_eq(out, &a);

    ck_assert_int_eq(om_market_ring_dequeue(&ring, 0, &out), 1);
    ck_assert_ptr_eq(out, &b);
    ck_assert_int_eq(om_market_ring_dequeue(&ring, 1, &out), 1);
    ck_assert_ptr_eq(out, &b);

    om_market_ring_destroy(&ring);
}
END_TEST

typedef struct OmMarketWaitCtx {
    OmMarketRing *ring;
    uint32_t consumer_index;
    size_t min_batch;
    int result;
} OmMarketWaitCtx;

static void *om_market_wait_thread(void *arg) {
    OmMarketWaitCtx *ctx = (OmMarketWaitCtx *)arg;
    ctx->result = om_market_ring_wait(ctx->ring, ctx->consumer_index, ctx->min_batch);
    return NULL;
}

START_TEST(test_market_ring_wait_notify) {
    OmMarketRing ring;
    OmMarketRingConfig config = {
        .capacity = 8,
        .consumer_count = 1,
        .notify_batch = 2
    };

    ck_assert_int_eq(om_market_ring_init(&ring, &config), 0);
    ck_assert_int_eq(om_market_ring_register_consumer(&ring, 0), 0);

    OmMarketWaitCtx ctx = {
        .ring = &ring,
        .consumer_index = 0,
        .min_batch = 2,
        .result = -1
    };
    pthread_t thread;
    ck_assert_int_eq(pthread_create(&thread, NULL, om_market_wait_thread, &ctx), 0);

    uint64_t a = 101;
    uint64_t b = 202;
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &a), 0);
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &b), 0);

    ck_assert_int_eq(pthread_join(thread, NULL), 0);
    ck_assert_int_eq(ctx.result, 0);

    void *out[2] = {0};
    int count = om_market_ring_dequeue_batch(&ring, 0, out, 2);
    ck_assert_int_eq(count, 2);
    ck_assert_ptr_eq(out[0], &a);
    ck_assert_ptr_eq(out[1], &b);

    om_market_ring_destroy(&ring);
}
END_TEST

START_TEST(test_market_ring_batch) {
    OmMarketRing ring;
    OmMarketRingConfig config = {
        .capacity = 8,
        .consumer_count = 1
    };

    ck_assert_int_eq(om_market_ring_init(&ring, &config), 0);
    ck_assert_int_eq(om_market_ring_register_consumer(&ring, 0), 0);

    uint64_t a = 11;
    uint64_t b = 22;
    uint64_t c = 33;
    void *out[4] = {0};

    ck_assert_int_eq(om_market_ring_enqueue(&ring, &a), 0);
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &b), 0);
    ck_assert_int_eq(om_market_ring_enqueue(&ring, &c), 0);

    int count = om_market_ring_dequeue_batch(&ring, 0, out, 4);
    ck_assert_int_eq(count, 3);
    ck_assert_ptr_eq(out[0], &a);
    ck_assert_ptr_eq(out[1], &b);
    ck_assert_ptr_eq(out[2], &c);

    om_market_ring_destroy(&ring);
}
END_TEST

static uint64_t test_marketable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx) {
    (void)ctx;
    return rec->org == viewer_org ? 0 : rec->vol_remain;
}

START_TEST(test_market_worker_dealable) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    OmMarketSubscription subs[2] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 16,
        .worker_count = 1,
        .org_to_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 4,
        .expected_subscribers_per_product = 1,
        .expected_price_levels = 4,
        .top_levels = 1,
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *worker = om_market_worker(&market, 0);
    ck_assert_ptr_nonnull(worker);

    OmWalInsert insert = {
        .order_id = 100,
        .price = 10,
        .volume = 50,
        .vol_remain = 50,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };

    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert), 0);

    uint64_t qty = 0;
    ck_assert_int_ne(om_market_worker_get_qty(worker, 1, 0, OM_SIDE_BID, 10, &qty), 0);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 10, &qty), 0);
    ck_assert_uint_eq(qty, 50);
    ck_assert_int_eq(om_market_worker_get_public_qty(worker, 0, OM_SIDE_BID, 10, &qty), 0);
    ck_assert_uint_eq(qty, 50);

    OmWalInsert insert2 = {
        .order_id = 101,
        .price = 9,
        .volume = 30,
        .vol_remain = 30,
        .org = 2,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert2), 0);
    ck_assert_int_ne(om_market_worker_get_public_qty(worker, 0, OM_SIDE_BID, 9, &qty), 0);
    ck_assert_int_ne(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 9, &qty), 0);
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 2, 0), 1);
    ck_assert_int_eq(om_market_worker_is_public_dirty(worker, 0), 1);
    ck_assert_int_eq(om_market_worker_clear_dirty(worker, 2, 0), 0);
    ck_assert_int_eq(om_market_worker_clear_public_dirty(worker, 0), 0);
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 2, 0), 0);
    ck_assert_int_eq(om_market_worker_is_public_dirty(worker, 0), 0);
    ck_assert_int_eq(om_market_worker_is_subscribed(worker, 2, 0), 1);
    ck_assert_int_eq(om_market_worker_is_subscribed(worker, 2, 1), 0);

    om_market_destroy(&market);
}
END_TEST

Suite* market_suite(void) {
    Suite *s = suite_create("market");
    TCase *tc_core = tcase_create("core");
    tcase_add_test(tc_core, test_market_ring_basic);
    tcase_add_test(tc_core, test_market_ring_batch);
    tcase_add_test(tc_core, test_market_ring_wait_notify);
    tcase_add_test(tc_core, test_market_worker_dealable);
    suite_add_tcase(s, tc_core);
    return s;
}
