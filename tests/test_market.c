#include <check.h>
#include <pthread.h>
#include <stdint.h>
#include "openmarket/om_worker.h"
#include "openmarket/om_market.h"
#include "openmatch/om_error.h"

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

/* Multi-org visibility: org 1's orders are visible to both org 2 and org 3 */
static uint64_t test_multi_org_marketable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx) {
    (void)ctx;
    if (rec->org == 1 && (viewer_org == 2 || viewer_org == 3)) {
        return rec->vol_remain;
    }
    return 0;
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
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
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
    ck_assert_int_eq(om_market_public_process(&market.public_workers[0], OM_WAL_INSERT, &insert), 0);

    uint64_t qty = 0;
    ck_assert_int_ne(om_market_worker_get_qty(worker, 1, 0, OM_SIDE_BID, 10, &qty), 0);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 10, &qty), 0);
    ck_assert_uint_eq(qty, 50);
    ck_assert_int_eq(om_market_public_get_qty(&market.public_workers[0], 0, OM_SIDE_BID, 10, &qty), 0);
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
    ck_assert_int_eq(om_market_public_process(&market.public_workers[0], OM_WAL_INSERT, &insert2), 0);
    ck_assert_int_ne(om_market_public_get_qty(&market.public_workers[0], 0, OM_SIDE_BID, 9, &qty), 0);
    ck_assert_int_ne(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 9, &qty), 0);
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 2, 0), 1);
    ck_assert_int_eq(om_market_public_is_dirty(&market.public_workers[0], 0), 1);
    ck_assert_int_eq(om_market_worker_clear_dirty(worker, 2, 0), 0);
    ck_assert_int_eq(om_market_public_clear_dirty(&market.public_workers[0], 0), 0);
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 2, 0), 0);
    ck_assert_int_eq(om_market_public_is_dirty(&market.public_workers[0], 0), 0);
    OmMarketDelta deltas[4];
    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 0, OM_SIDE_BID), 2);
    ck_assert_int_eq(om_market_public_copy_deltas(&market.public_workers[0], 0, OM_SIDE_BID, deltas, 4), 2);
    bool found_10 = false;
    for (int i = 0; i < 2; i++) {
        if (deltas[i].price == 10 && deltas[i].delta == 50) {
            found_10 = true;
        }
    }
    ck_assert(found_10);
    ck_assert_int_eq(om_market_public_copy_full(&market.public_workers[0], 0, OM_SIDE_BID, deltas, 4), 1);
    ck_assert_uint_eq(deltas[0].price, 10);
    ck_assert_int_eq(deltas[0].delta, 50);
    ck_assert_int_eq(om_market_public_clear_deltas(&market.public_workers[0], 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_worker_is_subscribed(worker, 2, 0), 1);
    ck_assert_int_eq(om_market_worker_is_subscribed(worker, 2, 1), 0);

    om_market_destroy(&market);
}
END_TEST

START_TEST(test_market_publish_combos) {
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
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
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
        .order_id = 200,
        .price = 20,
        .volume = 40,
        .vol_remain = 40,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert), 0);
    ck_assert_int_eq(om_market_public_process(&market.public_workers[0], OM_WAL_INSERT, &insert), 0);

    OmWalInsert insert_unsub = {
        .order_id = 201,
        .price = 21,
        .volume = 10,
        .vol_remain = 10,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 1
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert_unsub), 0);
    ck_assert_int_eq(om_market_public_process(&market.public_workers[0], OM_WAL_INSERT,
                                               &insert_unsub), 0);

    OmMarketDelta deltas[4];
    ck_assert_int_eq(om_market_worker_delta_count(worker, 2, 0, OM_SIDE_BID), 1);
    ck_assert_int_eq(om_market_worker_copy_deltas(worker, 2, 0, OM_SIDE_BID, deltas, 4), 1);
    ck_assert_uint_eq(deltas[0].price, 20);
    ck_assert_int_eq(deltas[0].delta, 40);
    ck_assert_int_eq(om_market_worker_clear_deltas(worker, 2, 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_worker_delta_count(worker, 2, 0, OM_SIDE_BID), 0);

    ck_assert_int_eq(om_market_worker_copy_full(worker, 2, 0, OM_SIDE_BID, deltas, 4), 1);
    ck_assert_uint_eq(deltas[0].price, 20);
    ck_assert_int_eq(deltas[0].delta, 40);
    ck_assert_int_eq(om_market_worker_clear_dirty(worker, 2, 0), 0);

    ck_assert_int_eq(om_market_worker_delta_count(worker, 2, 1, OM_SIDE_BID), OM_ERR_NOT_SUBSCRIBED);
    ck_assert_int_eq(om_market_worker_copy_deltas(worker, 2, 1, OM_SIDE_BID, deltas, 4), OM_ERR_NOT_SUBSCRIBED);
    ck_assert_int_eq(om_market_worker_copy_full(worker, 2, 1, OM_SIDE_BID, deltas, 4), OM_ERR_NOT_SUBSCRIBED);

    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 0, OM_SIDE_BID), 1);
    ck_assert_int_eq(om_market_public_copy_deltas(&market.public_workers[0], 0, OM_SIDE_BID,
                                                  deltas, 4), 1);
    ck_assert_uint_eq(deltas[0].price, 20);
    ck_assert_int_eq(deltas[0].delta, 40);
    ck_assert_int_eq(om_market_public_clear_deltas(&market.public_workers[0], 0, OM_SIDE_BID), 0);
    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 0, OM_SIDE_BID), 0);

    ck_assert_int_eq(om_market_public_copy_full(&market.public_workers[0], 0, OM_SIDE_BID,
                                                deltas, 4), 1);
    ck_assert_uint_eq(deltas[0].price, 20);
    ck_assert_int_eq(deltas[0].delta, 40);
    ck_assert_int_eq(om_market_public_clear_dirty(&market.public_workers[0], 0), 0);

    ck_assert_int_eq(om_market_public_delta_count(&market.public_workers[0], 1, OM_SIDE_BID), OM_ERR_NOT_SUBSCRIBED);
    ck_assert_int_eq(om_market_public_copy_deltas(&market.public_workers[0], 1, OM_SIDE_BID,
                                                  deltas, 4), OM_ERR_NOT_SUBSCRIBED);
    ck_assert_int_eq(om_market_public_copy_full(&market.public_workers[0], 1, OM_SIDE_BID,
                                                deltas, 4), OM_ERR_NOT_SUBSCRIBED);

    om_market_destroy(&market);
}
END_TEST

/* Test that CANCEL affects ALL orgs that can see the order */
START_TEST(test_market_multi_org_visibility) {
    OmMarket market;
    uint32_t org_to_worker[UINT16_MAX + 1U];
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        org_to_worker[i] = 0;
    }
    /* 3 orgs subscribed to product 0 */
    OmMarketSubscription subs[3] = {
        {.org_id = 1, .product_id = 0},
        {.org_id = 2, .product_id = 0},
        {.org_id = 3, .product_id = 0}
    };
    OmMarketConfig cfg = {
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 3,
        .expected_orders_per_worker = 4,
        .expected_subscribers_per_product = 3,
        .expected_price_levels = 4,
        .top_levels = 2,
        .dealable = test_multi_org_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *worker = om_market_worker(&market, 0);
    ck_assert_ptr_nonnull(worker);

    /* Insert order from org 1 - should be visible to both org 2 and org 3 */
    OmWalInsert insert = {
        .order_id = 300,
        .price = 100,
        .volume = 50,
        .vol_remain = 50,
        .org = 1,
        .flags = OM_SIDE_BID,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &insert), 0);

    /* Verify org 2 sees the order */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 50);

    /* Verify org 3 also sees the order */
    ck_assert_int_eq(om_market_worker_get_qty(worker, 3, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 50);

    /* Org 1 should NOT see its own order (dealable returns 0) */
    ck_assert_int_ne(om_market_worker_get_qty(worker, 1, 0, OM_SIDE_BID, 100, &qty), 0);

    /* Clear dirty flags before testing CANCEL */
    om_market_worker_clear_dirty(worker, 2, 0);
    om_market_worker_clear_dirty(worker, 3, 0);

    /* CANCEL - should remove qty from BOTH org 2 and org 3 */
    OmWalCancel cancel = {
        .order_id = 300,
        .product_id = 0
    };
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_CANCEL, &cancel), 0);

    /* Both orgs should now see 0 qty at this price */
    ck_assert_int_ne(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_int_ne(om_market_worker_get_qty(worker, 3, 0, OM_SIDE_BID, 100, &qty), 0);

    /* Both ladders should be marked dirty */
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 2, 0), 1);
    ck_assert_int_eq(om_market_worker_is_dirty(worker, 3, 0), 1);

    om_market_destroy(&market);
}
END_TEST

/* Test top-N promotion: when a level is removed, next-best should be promoted */
START_TEST(test_market_top_n_promotion) {
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
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 3,  /* Only top 3 price levels */
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketWorker *worker = om_market_worker(&market, 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];
    ck_assert_ptr_nonnull(worker);

    /* Insert 4 bid orders at prices 100, 90, 80, 70 (from org 1, visible to org 2) */
    /* For bids, best = highest, so top-3 should be 100, 90, 80 */
    OmWalInsert orders[4] = {
        {.order_id = 1, .price = 100, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 2, .price = 90, .volume = 20, .vol_remain = 20, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 3, .price = 80, .volume = 30, .vol_remain = 30, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 4, .price = 70, .volume = 40, .vol_remain = 40, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0}
    };

    for (int i = 0; i < 4; i++) {
        ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_INSERT, &orders[i]), 0);
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &orders[i]), 0);
    }

    /* Verify top-3 for private worker (org 2 viewing org 1's orders) */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 90, &qty), 0);
    ck_assert_uint_eq(qty, 20);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 30);
    /* Price 70 should NOT be in top-3 */
    ck_assert_int_ne(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 70, &qty), 0);

    /* Same for public worker */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 90, &qty), 0);
    ck_assert_uint_eq(qty, 20);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 30);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 70, &qty), 0);

    /* Cancel order at price 90 */
    OmWalCancel cancel = {.order_id = 2, .product_id = 0};
    ck_assert_int_eq(om_market_worker_process(worker, OM_WAL_CANCEL, &cancel), 0);
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);

    /* After removing price 90, price 70 should be PROMOTED into top-3 */
    /* New top-3 should be: 100, 80, 70 */
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_ne(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 90, &qty), 0);  /* Removed */
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 30);
    ck_assert_int_eq(om_market_worker_get_qty(worker, 2, 0, OM_SIDE_BID, 70, &qty), 0);  /* PROMOTED! */
    ck_assert_uint_eq(qty, 40);

    /* Same for public worker */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 90, &qty), 0);  /* Removed */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 80, &qty), 0);
    ck_assert_uint_eq(qty, 30);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 70, &qty), 0);  /* PROMOTED! */
    ck_assert_uint_eq(qty, 40);

    /* Verify full snapshot shows 3 levels */
    OmMarketDelta full[4];
    int n = om_market_public_copy_full(pub, 0, OM_SIDE_BID, full, 4);
    ck_assert_int_eq(n, 3);
    /* Should be sorted: 100, 80, 70 (bids descending) */
    ck_assert_uint_eq(full[0].price, 100);
    ck_assert_int_eq(full[0].delta, 10);
    ck_assert_uint_eq(full[1].price, 80);
    ck_assert_int_eq(full[1].delta, 30);
    ck_assert_uint_eq(full[2].price, 70);
    ck_assert_int_eq(full[2].delta, 40);

    om_market_destroy(&market);
}
END_TEST

/* Test top-N promotion for ask side (ascending order) */
START_TEST(test_market_top_n_promotion_ask) {
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
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 3,  /* Only top 3 price levels */
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Insert 4 ask orders at prices 10, 20, 30, 40 (ascending) */
    /* For asks, best = lowest, so top-3 should be 10, 20, 30 */
    OmWalInsert orders[4] = {
        {.order_id = 1, .price = 10, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0},
        {.order_id = 2, .price = 20, .volume = 20, .vol_remain = 20, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0},
        {.order_id = 3, .price = 30, .volume = 30, .vol_remain = 30, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0},
        {.order_id = 4, .price = 40, .volume = 40, .vol_remain = 40, .org = 1,
         .flags = OM_SIDE_ASK, .product_id = 0}
    };

    for (int i = 0; i < 4; i++) {
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &orders[i]), 0);
    }

    /* Verify top-3: 10, 20, 30 (not 40) */
    uint64_t qty = 0;
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 10, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 20, &qty), 0);
    ck_assert_uint_eq(qty, 20);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 30, &qty), 0);
    ck_assert_uint_eq(qty, 30);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 40, &qty), 0);  /* Outside top-3 */

    /* Cancel order at price 20 */
    OmWalCancel cancel = {.order_id = 2, .product_id = 0};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_CANCEL, &cancel), 0);

    /* After removing price 20, price 40 should be PROMOTED */
    /* New top-3 should be: 10, 30, 40 */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 10, &qty), 0);
    ck_assert_uint_eq(qty, 10);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 20, &qty), 0);  /* Removed */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 30, &qty), 0);
    ck_assert_uint_eq(qty, 30);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_ASK, 40, &qty), 0);  /* PROMOTED! */
    ck_assert_uint_eq(qty, 40);

    /* Verify full snapshot shows 3 levels sorted ascending */
    OmMarketDelta full[4];
    int n = om_market_public_copy_full(pub, 0, OM_SIDE_ASK, full, 4);
    ck_assert_int_eq(n, 3);
    ck_assert_uint_eq(full[0].price, 10);
    ck_assert_uint_eq(full[1].price, 30);
    ck_assert_uint_eq(full[2].price, 40);

    om_market_destroy(&market);
}
END_TEST

/* Test top-N promotion with MATCH (partial fill removing a level) */
START_TEST(test_market_top_n_promotion_match) {
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
        .max_products = 4,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = org_to_worker,
        .product_to_public_worker = org_to_worker,
        .subs = subs,
        .sub_count = 2,
        .expected_orders_per_worker = 8,
        .expected_subscribers_per_product = 2,
        .expected_price_levels = 8,
        .top_levels = 2,  /* Only top 2 price levels */
        .dealable = test_marketable,
        .dealable_ctx = NULL
    };

    ck_assert_int_eq(om_market_init(&market, &cfg), 0);
    OmMarketPublicWorker *pub = &market.public_workers[0];

    /* Insert 3 bid orders: top-2 should be 100, 90 (not 80) */
    OmWalInsert orders[3] = {
        {.order_id = 1, .price = 100, .volume = 10, .vol_remain = 10, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 2, .price = 90, .volume = 20, .vol_remain = 20, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0},
        {.order_id = 3, .price = 80, .volume = 30, .vol_remain = 30, .org = 1,
         .flags = OM_SIDE_BID, .product_id = 0}
    };

    for (int i = 0; i < 3; i++) {
        ck_assert_int_eq(om_market_public_process(pub, OM_WAL_INSERT, &orders[i]), 0);
    }

    uint64_t qty = 0;
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 90, &qty), 0);
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 80, &qty), 0);  /* Outside */

    /* Match fully consumes order at price 100 */
    OmWalMatch match = {.maker_id = 1, .taker_id = 999, .volume = 10, .price = 100};
    ck_assert_int_eq(om_market_public_process(pub, OM_WAL_MATCH, &match), 0);

    /* After removing price 100, price 80 should be promoted */
    ck_assert_int_ne(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 100, &qty), 0);  /* Removed */
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 90, &qty), 0);
    ck_assert_uint_eq(qty, 20);
    ck_assert_int_eq(om_market_public_get_qty(pub, 0, OM_SIDE_BID, 80, &qty), 0);  /* PROMOTED! */
    ck_assert_uint_eq(qty, 30);

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
    tcase_add_test(tc_core, test_market_publish_combos);
    tcase_add_test(tc_core, test_market_multi_org_visibility);
    tcase_add_test(tc_core, test_market_top_n_promotion);
    tcase_add_test(tc_core, test_market_top_n_promotion_ask);
    tcase_add_test(tc_core, test_market_top_n_promotion_match);
    suite_add_tcase(s, tc_core);
    return s;
}
