#include <check.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include "openmatch/orderbook.h"

/* Test orderbook context initialization */
START_TEST(test_orderbook_init)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    int ret = om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);
    ck_assert_int_eq(ret, 0);

    /* Verify product books are initialized */
    for (int i = 0; i < 10; i++) {
        ck_assert_uint_eq(ctx.products[i].bid_head_q1, OM_SLOT_IDX_NULL);
        ck_assert_uint_eq(ctx.products[i].ask_head_q1, OM_SLOT_IDX_NULL);
    }

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test inserting bid orders */
START_TEST(test_orderbook_insert_bid)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    /* Create a bid order */
    OmSlabSlot *order = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(order);

    uint32_t order_id = om_slab_next_order_id(&ctx.slab);
    om_slot_set_order_id(order, order_id);
    om_slot_set_price(order, 10000);
    om_slot_set_volume(order, 100);
    om_slot_set_volume_remain(order, 100);
    om_slot_set_flags(order, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(order, 1);

    /* Insert order */
    int ret = om_orderbook_insert(&ctx, 0, order);
    ck_assert_int_eq(ret, 0);

    /* Check best bid */
    uint64_t best_bid = om_orderbook_get_best_bid(&ctx, 0);
    ck_assert_uint_eq(best_bid, 10000);

    /* Check volume at price */
    uint64_t volume = om_orderbook_get_volume_at_price(&ctx, 0, 10000, true);
    ck_assert_uint_eq(volume, 100);

    /* Check price level exists */
    ck_assert(om_orderbook_price_level_exists(&ctx, 0, 10000, true));

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test inserting multiple bid orders at same price */
START_TEST(test_orderbook_insert_multiple_bids_same_price)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    /* Create three bid orders at same price */
    for (int i = 0; i < 3; i++) {
        OmSlabSlot *order = om_slab_alloc(&ctx.slab);
        ck_assert_ptr_nonnull(order);

        uint32_t order_id = om_slab_next_order_id(&ctx.slab);
        om_slot_set_order_id(order, order_id);
        om_slot_set_price(order, 10000);
        om_slot_set_volume(order, 50);
        om_slot_set_volume_remain(order, 50);
        om_slot_set_flags(order, OM_SIDE_BID | OM_TYPE_LIMIT);
        om_slot_set_org(order, 1);

        int ret = om_orderbook_insert(&ctx, 0, order);
        ck_assert_int_eq(ret, 0);
    }

    /* Check volume accumulates */
    uint64_t volume = om_orderbook_get_volume_at_price(&ctx, 0, 10000, true);
    ck_assert_uint_eq(volume, 150);

    /* Check price level count */
    uint32_t count = om_orderbook_get_price_level_count(&ctx, 0, true);
    ck_assert_uint_eq(count, 1);

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test inserting bid orders at different prices (sorted) */
START_TEST(test_orderbook_insert_bids_sorted)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    /* Insert bids at different prices */
    uint64_t prices[] = {9900, 10100, 10000};
    
    for (int i = 0; i < 3; i++) {
        OmSlabSlot *order = om_slab_alloc(&ctx.slab);
        ck_assert_ptr_nonnull(order);

        uint32_t order_id = om_slab_next_order_id(&ctx.slab);
        om_slot_set_order_id(order, order_id);
        om_slot_set_price(order, prices[i]);
        om_slot_set_volume(order, 100);
        om_slot_set_volume_remain(order, 100);
        om_slot_set_flags(order, OM_SIDE_BID | OM_TYPE_LIMIT);
        om_slot_set_org(order, 1);

        int ret = om_orderbook_insert(&ctx, 0, order);
        ck_assert_int_eq(ret, 0);
    }

    /* Best bid should be 10100 (highest) */
    uint64_t best_bid = om_orderbook_get_best_bid(&ctx, 0);
    ck_assert_uint_eq(best_bid, 10100);

    /* Should have 3 price levels */
    uint32_t count = om_orderbook_get_price_level_count(&ctx, 0, true);
    ck_assert_uint_eq(count, 3);

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test inserting ask orders */
START_TEST(test_orderbook_insert_ask)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    /* Create an ask order */
    OmSlabSlot *order = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(order);

    uint32_t order_id = om_slab_next_order_id(&ctx.slab);
    om_slot_set_order_id(order, order_id);
    om_slot_set_price(order, 10100);
    om_slot_set_volume(order, 100);
    om_slot_set_volume_remain(order, 100);
    om_slot_set_flags(order, OM_SIDE_ASK | OM_TYPE_LIMIT);
    om_slot_set_org(order, 1);

    /* Insert order */
    int ret = om_orderbook_insert(&ctx, 0, order);
    ck_assert_int_eq(ret, 0);

    /* Check best ask */
    uint64_t best_ask = om_orderbook_get_best_ask(&ctx, 0);
    ck_assert_uint_eq(best_ask, 10100);

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test inserting ask orders at different prices (sorted ascending) */
START_TEST(test_orderbook_insert_asks_sorted)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    /* Insert asks at different prices */
    uint64_t prices[] = {10200, 10000, 10100};
    
    for (int i = 0; i < 3; i++) {
        OmSlabSlot *order = om_slab_alloc(&ctx.slab);
        ck_assert_ptr_nonnull(order);

        uint32_t order_id = om_slab_next_order_id(&ctx.slab);
        om_slot_set_order_id(order, order_id);
        om_slot_set_price(order, prices[i]);
        om_slot_set_volume(order, 100);
        om_slot_set_volume_remain(order, 100);
        om_slot_set_flags(order, OM_SIDE_ASK | OM_TYPE_LIMIT);
        om_slot_set_org(order, 1);

        int ret = om_orderbook_insert(&ctx, 0, order);
        ck_assert_int_eq(ret, 0);
    }

    /* Best ask should be 10000 (lowest) */
    uint64_t best_ask = om_orderbook_get_best_ask(&ctx, 0);
    ck_assert_uint_eq(best_ask, 10000);

    /* Should have 3 price levels */
    uint32_t count = om_orderbook_get_price_level_count(&ctx, 0, false);
    ck_assert_uint_eq(count, 3);

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test cancel order */
START_TEST(test_orderbook_cancel)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    /* Create and insert a bid order */
    OmSlabSlot *order = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(order);

    uint32_t order_id = om_slab_next_order_id(&ctx.slab);
    om_slot_set_order_id(order, order_id);
    om_slot_set_price(order, 10000);
    om_slot_set_volume(order, 100);
    om_slot_set_volume_remain(order, 100);
    om_slot_set_flags(order, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(order, 1);

    int ret = om_orderbook_insert(&ctx, 0, order);
    ck_assert_int_eq(ret, 0);

    /* Verify order is there */
    ck_assert_uint_eq(om_orderbook_get_best_bid(&ctx, 0), 10000);

    /* Cancel the order using only order_id (product_id is stored in hashmap) */
    bool cancelled = om_orderbook_cancel(&ctx, order_id);
    ck_assert(cancelled);

    /* Verify order is gone from orderbook */
    ck_assert_uint_eq(om_orderbook_get_best_bid(&ctx, 0), 0);
    ck_assert(!om_orderbook_price_level_exists(&ctx, 0, 10000, true));

    /* Verify order is gone from hashmap */
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&ctx, order_id));

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test cancel one of multiple orders at same price */
START_TEST(test_orderbook_cancel_partial)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    /* Create two bid orders at same price - store order IDs */
    uint32_t order_id1 = om_slab_next_order_id(&ctx.slab);
    OmSlabSlot *order1 = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(order1);
    om_slot_set_order_id(order1, order_id1);
    om_slot_set_price(order1, 10000);
    om_slot_set_volume(order1, 100);
    om_slot_set_volume_remain(order1, 100);
    om_slot_set_flags(order1, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(order1, 1);
    om_orderbook_insert(&ctx, 0, order1);

    uint32_t order_id2 = om_slab_next_order_id(&ctx.slab);
    OmSlabSlot *order2 = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(order2);
    om_slot_set_order_id(order2, order_id2);
    om_slot_set_price(order2, 10000);
    om_slot_set_volume(order2, 50);
    om_slot_set_volume_remain(order2, 50);
    om_slot_set_flags(order2, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(order2, 1);
    om_orderbook_insert(&ctx, 0, order2);

    /* Verify total volume */
    ck_assert_uint_eq(om_orderbook_get_volume_at_price(&ctx, 0, 10000, true), 150);

    /* Verify both orders are in hashmap */
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&ctx, order_id1));
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&ctx, order_id2));

    /* Cancel first order using only order_id */
    bool cancelled = om_orderbook_cancel(&ctx, order_id1);
    ck_assert(cancelled);

    /* Verify price level still exists with remaining volume */
    ck_assert(om_orderbook_price_level_exists(&ctx, 0, 10000, true));
    ck_assert_uint_eq(om_orderbook_get_volume_at_price(&ctx, 0, 10000, true), 50);
    ck_assert_uint_eq(om_orderbook_get_best_bid(&ctx, 0), 10000);

    /* Verify order1 is gone from hashmap but order2 remains */
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&ctx, order_id1));
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&ctx, order_id2));

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test canceling head at best price promotes next level */
START_TEST(test_orderbook_cancel_best_price)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    OmSlabSlot *best = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(best);
    uint32_t best_id = om_slab_next_order_id(&ctx.slab);
    om_slot_set_order_id(best, best_id);
    om_slot_set_price(best, 10100);
    om_slot_set_volume(best, 100);
    om_slot_set_volume_remain(best, 100);
    om_slot_set_flags(best, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(best, 1);
    om_orderbook_insert(&ctx, 0, best);

    OmSlabSlot *next = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(next);
    uint32_t next_id = om_slab_next_order_id(&ctx.slab);
    om_slot_set_order_id(next, next_id);
    om_slot_set_price(next, 10000);
    om_slot_set_volume(next, 50);
    om_slot_set_volume_remain(next, 50);
    om_slot_set_flags(next, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(next, 1);
    om_orderbook_insert(&ctx, 0, next);

    ck_assert_uint_eq(om_orderbook_get_best_bid(&ctx, 0), 10100);

    ck_assert(om_orderbook_cancel(&ctx, best_id));

    ck_assert_uint_eq(om_orderbook_get_best_bid(&ctx, 0), 10000);
    ck_assert_uint_eq(om_orderbook_get_price_level_count(&ctx, 0, true), 1);

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test cancel head with 3+ orders at same price (tail pointer) */
START_TEST(test_orderbook_cancel_head_same_price_tail)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    uint32_t order_ids[3];
    for (int i = 0; i < 3; i++) {
        OmSlabSlot *order = om_slab_alloc(&ctx.slab);
        ck_assert_ptr_nonnull(order);
        order_ids[i] = om_slab_next_order_id(&ctx.slab);
        om_slot_set_order_id(order, order_ids[i]);
        om_slot_set_price(order, 10000);
        om_slot_set_volume(order, 10);
        om_slot_set_volume_remain(order, 10);
        om_slot_set_flags(order, OM_SIDE_BID | OM_TYPE_LIMIT);
        om_slot_set_org(order, 1);
        ck_assert_int_eq(om_orderbook_insert(&ctx, 0, order), 0);
    }

    /* Cancel head (first order at that price) */
    ck_assert(om_orderbook_cancel(&ctx, order_ids[0]));

    /* Remaining volume should be 20 at that price */
    ck_assert_uint_eq(om_orderbook_get_volume_at_price(&ctx, 0, 10000, true), 20);

    /* Cancel promoted head and ensure one order remains */
    ck_assert(om_orderbook_cancel(&ctx, order_ids[1]));
    ck_assert_uint_eq(om_orderbook_get_volume_at_price(&ctx, 0, 10000, true), 10);

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test multiple products */
START_TEST(test_orderbook_multiple_products)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    /* Insert orders into two different products */
    for (int prod = 0; prod < 2; prod++) {
        OmSlabSlot *bid = om_slab_alloc(&ctx.slab);
        ck_assert_ptr_nonnull(bid);
        om_slot_set_order_id(bid, om_slab_next_order_id(&ctx.slab));
        om_slot_set_price(bid, 10000 + prod * 100);
        om_slot_set_volume(bid, 100);
        om_slot_set_volume_remain(bid, 100);
        om_slot_set_flags(bid, OM_SIDE_BID | OM_TYPE_LIMIT);
        om_slot_set_org(bid, 1);
        om_orderbook_insert(&ctx, prod, bid);
    }

    /* Verify each product has correct best bid */
    ck_assert_uint_eq(om_orderbook_get_best_bid(&ctx, 0), 10000);
    ck_assert_uint_eq(om_orderbook_get_best_bid(&ctx, 1), 10100);

    om_orderbook_destroy(&ctx);
}
END_TEST

/* Test hashmap order lookup functionality */
START_TEST(test_orderbook_hashmap_lookup)
{
    OmOrderbookContext ctx;
    OmSlabConfig config = {
        .user_data_size = 64,
        .aux_data_size = 128,
        .total_slots = 1000
    };

    om_orderbook_init(&ctx, &config, NULL, 10, 100, 0);

    /* Create and insert an order */
    uint32_t order_id = om_slab_next_order_id(&ctx.slab);
    OmSlabSlot *order = om_slab_alloc(&ctx.slab);
    ck_assert_ptr_nonnull(order);
    om_slot_set_order_id(order, order_id);
    om_slot_set_price(order, 10000);
    om_slot_set_volume(order, 100);
    om_slot_set_volume_remain(order, 100);
    om_slot_set_flags(order, OM_SIDE_BID | OM_TYPE_LIMIT);
    om_slot_set_org(order, 1);
    
    int ret = om_orderbook_insert(&ctx, 0, order);
    ck_assert_int_eq(ret, 0);

    /* Look up order by ID using hashmap */
    OmSlabSlot *found = om_orderbook_get_slot_by_id(&ctx, order_id);
    ck_assert_ptr_nonnull(found);
    ck_assert_uint_eq(found->order_id, order_id);
    ck_assert_uint_eq(found->price, 10000);
    ck_assert_uint_eq(found->volume, 100);

    /* Cancel the order using only order_id */
    bool cancelled = om_orderbook_cancel(&ctx, order_id);
    ck_assert(cancelled);

    /* Verify order is no longer in hashmap */
    found = om_orderbook_get_slot_by_id(&ctx, order_id);
    ck_assert_ptr_null(found);

    /* Verify non-existent order returns NULL */
    found = om_orderbook_get_slot_by_id(&ctx, 99999);
    ck_assert_ptr_null(found);

    om_orderbook_destroy(&ctx);
}
END_TEST

Suite *orderbook_suite(void)
{
    Suite *s = suite_create("Orderbook");
    TCase *tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_orderbook_init);
    tcase_add_test(tc_core, test_orderbook_insert_bid);
    tcase_add_test(tc_core, test_orderbook_insert_multiple_bids_same_price);
    tcase_add_test(tc_core, test_orderbook_insert_bids_sorted);
    tcase_add_test(tc_core, test_orderbook_insert_ask);
    tcase_add_test(tc_core, test_orderbook_insert_asks_sorted);
    tcase_add_test(tc_core, test_orderbook_cancel);
    tcase_add_test(tc_core, test_orderbook_cancel_partial);
    tcase_add_test(tc_core, test_orderbook_cancel_best_price);
    tcase_add_test(tc_core, test_orderbook_cancel_head_same_price_tail);
    tcase_add_test(tc_core, test_orderbook_multiple_products);
    tcase_add_test(tc_core, test_orderbook_hashmap_lookup);

    suite_add_tcase(s, tc_core);
    return s;
}
