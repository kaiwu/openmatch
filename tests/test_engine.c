#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "openmatch/om_engine.h"

static void test_engine_init_destroy(void) {
    printf("Testing engine init/destroy...\n");

    OmEngine engine;
    int ret = om_engine_init(&engine);
    assert(ret == 0);

    om_engine_destroy(&engine);
    printf("  PASS\n");
}

static void test_product_management(void) {
    printf("Testing product management...\n");

    OmEngine engine;
    om_engine_init(&engine);

    OmProductBook *book = om_engine_add_product(&engine, 12345);
    assert(book != NULL);
    assert(book->product_id == 12345);

    OmProductBook *retrieved = om_engine_get_product(&engine, 12345);
    assert(retrieved == book);

    OmProductBook *book2 = om_engine_add_product(&engine, 67890);
    assert(book2 != NULL);
    assert(book2 != book);

    OmProductBook *duplicate = om_engine_add_product(&engine, 12345);
    assert(duplicate == NULL);

    bool removed = om_engine_remove_product(&engine, 12345);
    assert(removed);

    OmProductBook *not_found = om_engine_get_product(&engine, 12345);
    assert(not_found == NULL);

    om_engine_destroy(&engine);
    printf("  PASS\n");
}

static void test_order_placement(void) {
    printf("Testing order placement...\n");

    OmEngine engine;
    om_engine_init(&engine);

    om_engine_add_product(&engine, 1);

    OmOrder bid_order = {
        .price = 100,
        .quantity = 10,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };

    OmMatchResult results[10];
    int num_matches = om_engine_place_order(&engine, 1, &bid_order, results, 10);
    assert(num_matches == 0);
    assert(bid_order.order_id != 0);

    OmOrder ask_order = {
        .price = 100,
        .quantity = 5,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_LIMIT
    };

    num_matches = om_engine_place_order(&engine, 1, &ask_order, results, 10);
    assert(num_matches == 1);
    assert(results[0].quantity == 5);
    assert(results[0].price == 100);

    om_engine_destroy(&engine);
    printf("  PASS\n");
}

static void test_partial_fill(void) {
    printf("Testing partial fill...\n");

    OmEngine engine;
    om_engine_init(&engine);

    om_engine_add_product(&engine, 1);

    OmOrder bid_order = {
        .price = 100,
        .quantity = 10,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };

    OmMatchResult results[10];
    om_engine_place_order(&engine, 1, &bid_order, results, 10);

    OmOrder ask_order = {
        .price = 100,
        .quantity = 15,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_LIMIT
    };

    int num_matches = om_engine_place_order(&engine, 1, &ask_order, results, 10);
    assert(num_matches == 1);
    assert(results[0].quantity == 10);

    om_engine_destroy(&engine);
    printf("  PASS\n");
}

static void test_price_priority(void) {
    printf("Testing price priority...\n");

    OmEngine engine;
    om_engine_init(&engine);

    om_engine_add_product(&engine, 1);

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

    OmOrder ask = {
        .price = 101,
        .quantity = 6,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_LIMIT
    };

    OmMatchResult results[10];
    int num_matches = om_engine_place_order(&engine, 1, &ask, results, 10);
    assert(num_matches == 2);

    om_engine_destroy(&engine);
    printf("  PASS\n");
}

static void test_time_priority(void) {
    printf("Testing time priority...\n");

    OmEngine engine;
    om_engine_init(&engine);

    om_engine_add_product(&engine, 1);

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

    OmOrder ask = {
        .price = 100,
        .quantity = 7,
        .side = OM_SIDE_ASK,
        .type = OM_ORDER_TYPE_LIMIT
    };

    OmMatchResult results[10];
    int num_matches = om_engine_place_order(&engine, 1, &ask, results, 10);
    assert(num_matches == 2);
    assert(results[0].quantity == 5);
    assert(results[1].quantity == 2);

    om_engine_destroy(&engine);
    printf("  PASS\n");
}

static void test_multiple_products(void) {
    printf("Testing multiple products...\n");

    OmEngine engine;
    om_engine_init(&engine);

    om_engine_add_product(&engine, 1);
    om_engine_add_product(&engine, 2);

    OmOrder order1 = {
        .price = 100,
        .quantity = 10,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    om_engine_place_order(&engine, 1, &order1, NULL, 0);

    OmOrder order2 = {
        .price = 200,
        .quantity = 10,
        .side = OM_SIDE_BID,
        .type = OM_ORDER_TYPE_LIMIT
    };
    om_engine_place_order(&engine, 2, &order2, NULL, 0);

    OmProductBook *book1 = om_engine_get_product(&engine, 1);
    OmProductBook *book2 = om_engine_get_product(&engine, 2);

    assert(book1->last_price == 0);
    assert(book2->last_price == 0);

    om_engine_destroy(&engine);
    printf("  PASS\n");
}

int main(void) {
    printf("\n=== OpenMatch Engine Tests ===\n\n");

    test_engine_init_destroy();
    test_product_management();
    test_order_placement();
    test_partial_fill();
    test_price_priority();
    test_time_priority();
    test_multiple_products();

    printf("\n=== All tests passed! ===\n");
    return 0;
}
