#include <check.h>
#include <stdlib.h>
#include <string.h>
#include "openmatch/om_engine.h"

START_TEST(test_engine_init)
{
    OmEngine engine;
    int ret = om_engine_init(&engine);
    ck_assert_int_eq(ret, 0);
    ck_assert_uint_eq(engine.next_order_id, 1);
    ck_assert_uint_eq(engine.timestamp_counter, 0);
    ck_assert_ptr_nonnull(engine.products);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_init_null)
{
    int ret = om_engine_init(NULL);
    ck_assert_int_eq(ret, -1);
}
END_TEST

START_TEST(test_engine_destroy_null)
{
    // Should not crash
    om_engine_destroy(NULL);
}
END_TEST

START_TEST(test_product_add)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Add a product
    OmProductBook *book = om_engine_add_product(&engine, 1);
    ck_assert_ptr_nonnull(book);
    ck_assert_uint_eq(book->product_id, 1);
    ck_assert_ptr_nonnull(book->bid_levels);
    ck_assert_ptr_nonnull(book->ask_levels);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_product_add_duplicate)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Add first product
    OmProductBook *book1 = om_engine_add_product(&engine, 1);
    ck_assert_ptr_nonnull(book1);
    
    // Try to add duplicate
    OmProductBook *book2 = om_engine_add_product(&engine, 1);
    ck_assert_ptr_null(book2);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_product_get)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Add product
    OmProductBook *book_added = om_engine_add_product(&engine, 42);
    ck_assert_ptr_nonnull(book_added);
    
    // Get product
    OmProductBook *book_get = om_engine_get_product(&engine, 42);
    ck_assert_ptr_eq(book_get, book_added);
    
    // Get non-existent product
    OmProductBook *book_missing = om_engine_get_product(&engine, 999);
    ck_assert_ptr_null(book_missing);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_product_remove)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Add and remove product
    om_engine_add_product(&engine, 1);
    bool removed = om_engine_remove_product(&engine, 1);
    ck_assert(removed);
    
    // Verify removed
    OmProductBook *book = om_engine_get_product(&engine, 1);
    ck_assert_ptr_null(book);
    
    // Remove non-existent
    bool removed2 = om_engine_remove_product(&engine, 999);
    ck_assert(!removed2);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_order_id_generation)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Generate order IDs
    uint64_t id1 = om_engine_get_next_order_id(&engine);
    ck_assert_uint_eq(id1, 1);
    
    uint64_t id2 = om_engine_get_next_order_id(&engine);
    ck_assert_uint_eq(id2, 2);
    
    uint64_t id3 = om_engine_get_next_order_id(&engine);
    ck_assert_uint_eq(id3, 3);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_place_limit_order)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    OmOrder order = {
        .price = 100,
        .quantity = 10,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    OmMatchResult results[10];
    int num_matches = om_engine_place_order(&engine, 1, &order, results, 10);
    
    // No matches for first order in empty book
    ck_assert_int_eq(num_matches, 0);
    ck_assert_uint_eq(order.order_id, 1);
    ck_assert(order.timestamp > 0);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_place_matching_orders)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Place bid order
    OmOrder bid = {
        .price = 100,
        .quantity = 10,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    OmMatchResult results[10];
    int num_matches = om_engine_place_order(&engine, 1, &bid, results, 10);
    ck_assert_int_eq(num_matches, 0);
    
    // Place matching ask order
    OmOrder ask = {
        .price = 100,
        .quantity = 5,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    num_matches = om_engine_place_order(&engine, 1, &ask, results, 10);
    ck_assert_int_eq(num_matches, 1);
    ck_assert_uint_eq(results[0].price, 100);
    ck_assert_uint_eq(results[0].quantity, 5);
    ck_assert_uint_eq(results[0].maker_order_id, 1);  // bid was maker
    ck_assert_uint_eq(results[0].taker_order_id, 2);  // ask was taker
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_place_partial_fill)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Place large bid
    OmOrder bid = {
        .price = 100,
        .quantity = 10,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    OmMatchResult results[10];
    om_engine_place_order(&engine, 1, &bid, results, 10);
    
    // Place small ask that partially fills
    OmOrder ask = {
        .price = 100,
        .quantity = 3,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    int num_matches = om_engine_place_order(&engine, 1, &ask, results, 10);
    ck_assert_int_eq(num_matches, 1);
    ck_assert_uint_eq(results[0].quantity, 3);
    
    // Place another ask to fill remaining
    OmOrder ask2 = {
        .price = 100,
        .quantity = 7,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    num_matches = om_engine_place_order(&engine, 1, &ask2, results, 10);
    ck_assert_int_eq(num_matches, 1);
    ck_assert_uint_eq(results[0].quantity, 7);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_place_market_order)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Setup book with liquidity
    OmOrder bid1 = {
        .price = 100,
        .quantity = 5,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    OmMatchResult results[10];
    om_engine_place_order(&engine, 1, &bid1, results, 10);
    
    OmOrder bid2 = {
        .price = 99,
        .quantity = 5,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    om_engine_place_order(&engine, 1, &bid2, results, 10);
    
    // Market sell order
    OmOrder market_sell = {
        .price = 0,
        .quantity = 7,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_MARKET
    };
    
    int num_matches = om_engine_place_order(&engine, 1, &market_sell, results, 10);
    ck_assert_int_eq(num_matches, 2);
    ck_assert_uint_eq(results[0].quantity, 5);  // Filled against bid1
    ck_assert_uint_eq(results[1].quantity, 2);  // Partial fill against bid2
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_price_priority)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Add bids at different prices
    OmOrder bid1 = {
        .price = 100,
        .quantity = 5,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    om_engine_place_order(&engine, 1, &bid1, NULL, 0);
    
    OmOrder bid2 = {
        .price = 101,
        .quantity = 5,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    om_engine_place_order(&engine, 1, &bid2, NULL, 0);
    
    // Sell order should match with better price first
    OmOrder ask = {
        .price = 101,
        .quantity = 6,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    OmMatchResult results[10];
    int num_matches = om_engine_place_order(&engine, 1, &ask, results, 10);
    ck_assert_int_eq(num_matches, 1);
    ck_assert_uint_eq(results[0].price, 101);  // Better price matched first
    ck_assert_uint_eq(results[0].quantity, 5);  // Filled 5 units from bid at 101
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_time_priority)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Add two bids at same price
    OmOrder bid1 = {
        .price = 100,
        .quantity = 5,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    om_engine_place_order(&engine, 1, &bid1, NULL, 0);
    
    OmOrder bid2 = {
        .price = 100,
        .quantity = 5,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    om_engine_place_order(&engine, 1, &bid2, NULL, 0);
    
    // Sell order should fill first bid completely, then partial second
    OmOrder ask = {
        .price = 100,
        .quantity = 7,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    OmMatchResult results[10];
    int num_matches = om_engine_place_order(&engine, 1, &ask, results, 10);
    ck_assert_int_eq(num_matches, 2);
    ck_assert_uint_eq(results[0].quantity, 5);
    ck_assert_uint_eq(results[1].quantity, 2);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_multiple_products)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Add orders to different products
    OmOrder order1 = {
        .price = 100,
        .quantity = 10,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    OmOrder order2 = {
        .price = 200,
        .quantity = 20,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    
    OmMatchResult results[10];
    om_engine_place_order(&engine, 1, &order1, results, 10);
    om_engine_place_order(&engine, 2, &order2, results, 10);
    
    // Verify separate books
    OmProductBook *book1 = om_engine_get_product(&engine, 1);
    OmProductBook *book2 = om_engine_get_product(&engine, 2);
    
    ck_assert_ptr_nonnull(book1);
    ck_assert_ptr_nonnull(book2);
    ck_assert_ptr_ne(book1, book2);
    
    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_cancel_order)
{
    OmEngine engine;
    om_engine_init(&engine);
    
    // Add an order (cancel not fully implemented, but test it doesn't crash)
    bool cancelled = om_engine_cancel_order(&engine, 1, 1);
    // Currently returns false as it's not implemented
    ck_assert(!cancelled);
    
    om_engine_destroy(&engine);
}
END_TEST

Suite* engine_suite(void)
{
    Suite* s = suite_create("Engine");
    
    TCase* tc_init = tcase_create("Init");
    tcase_add_test(tc_init, test_engine_init);
    tcase_add_test(tc_init, test_engine_init_null);
    tcase_add_test(tc_init, test_engine_destroy_null);
    suite_add_tcase(s, tc_init);
    
    TCase* tc_product = tcase_create("Product");
    tcase_add_test(tc_product, test_product_add);
    tcase_add_test(tc_product, test_product_add_duplicate);
    tcase_add_test(tc_product, test_product_get);
    tcase_add_test(tc_product, test_product_remove);
    tcase_add_test(tc_product, test_multiple_products);
    suite_add_tcase(s, tc_product);
    
    TCase* tc_order = tcase_create("Order");
    tcase_add_test(tc_order, test_order_id_generation);
    tcase_add_test(tc_order, test_place_limit_order);
    tcase_add_test(tc_order, test_place_matching_orders);
    tcase_add_test(tc_order, test_place_partial_fill);
    tcase_add_test(tc_order, test_place_market_order);
    tcase_add_test(tc_order, test_price_priority);
    tcase_add_test(tc_order, test_time_priority);
    tcase_add_test(tc_order, test_cancel_order);
    suite_add_tcase(s, tc_order);
    
    return s;
}
