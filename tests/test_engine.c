#include <check.h>
#include <stdint.h>
#include "openmatch/om_engine.h"

typedef struct TestMatchCtx {
    uint64_t can_match_calls;
    uint64_t on_match_calls;
    uint64_t on_deal_calls;
    uint64_t on_booked_calls;
    uint64_t on_filled_calls;
    uint64_t on_cancel_calls;
    uint64_t can_match_cap;
    bool can_match_zero;
    bool can_match_skip_once;
    bool pre_booked_allow;
} TestMatchCtx;

static uint64_t test_can_match(const OmSlabSlot *maker, const OmSlabSlot *taker, void *user_ctx)
{
    (void)maker;
    (void)taker;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->can_match_calls++;
    if (ctx->can_match_skip_once) {
        ctx->can_match_skip_once = false;
        return 0;
    }
    if (ctx->can_match_zero) {
        return 0;
    }
    if (ctx->can_match_cap != 0) {
        return ctx->can_match_cap;
    }
    return UINT64_MAX;
}

static void test_on_match(const OmSlabSlot *order, uint64_t price, uint64_t qty, void *user_ctx)
{
    (void)order;
    (void)price;
    (void)qty;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->on_match_calls++;
}

static void test_on_deal(const OmSlabSlot *maker, const OmSlabSlot *taker,
                         uint64_t price, uint64_t qty, void *user_ctx)
{
    (void)maker;
    (void)taker;
    (void)price;
    (void)qty;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->on_deal_calls++;
}

static void test_on_booked(const OmSlabSlot *order, void *user_ctx)
{
    (void)order;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->on_booked_calls++;
}

static void test_on_filled(const OmSlabSlot *order, void *user_ctx)
{
    (void)order;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->on_filled_calls++;
}

static void test_on_cancel(const OmSlabSlot *order, void *user_ctx)
{
    (void)order;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    ctx->on_cancel_calls++;
}

static bool test_pre_booked(const OmSlabSlot *order, void *user_ctx)
{
    (void)order;
    TestMatchCtx *ctx = (TestMatchCtx *)user_ctx;
    return ctx->pre_booked_allow;
}

static void init_engine_with_ctx(OmEngine *engine, TestMatchCtx *ctx)
{
    OmEngineConfig config = {
        .slab = {
            .user_data_size = 64,
            .aux_data_size = 128,
            .total_slots = 1000
        },
        .wal = NULL,
        .callbacks = {
            .can_match = test_can_match,
            .on_match = test_on_match,
            .on_deal = test_on_deal,
            .on_booked = test_on_booked,
            .on_filled = test_on_filled,
            .on_cancel = test_on_cancel,
            .pre_booked = test_pre_booked,
            .user_ctx = ctx
        }
    };

    ck_assert_int_eq(om_engine_init(engine, &config), 0);
}

static OmSlabSlot *make_order(OmEngine *engine, uint64_t price, uint64_t volume, uint16_t flags)
{
    OmSlabSlot *order = om_slab_alloc(&engine->orderbook.slab);
    ck_assert_ptr_nonnull(order);
    om_slot_set_order_id(order, om_slab_next_order_id(&engine->orderbook.slab));
    om_slot_set_price(order, price);
    om_slot_set_volume(order, volume);
    om_slot_set_volume_remain(order, volume);
    om_slot_set_flags(order, flags);
    om_slot_set_org(order, 1);
    return order;
}

START_TEST(test_engine_init_callbacks)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);
    ck_assert_ptr_nonnull(om_engine_get_orderbook(&engine));
    ck_assert(om_engine_has_can_match(&engine));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_callback_context)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    engine.callbacks.can_match(NULL, NULL, &ctx);
    engine.callbacks.on_match(NULL, 0, 0, &ctx);
    engine.callbacks.on_deal(NULL, NULL, 0, 0, &ctx);
    engine.callbacks.on_booked(NULL, &ctx);
    engine.callbacks.on_filled(NULL, &ctx);
    engine.callbacks.on_cancel(NULL, &ctx);
    ck_assert_uint_eq(ctx.can_match_calls, 1);
    ck_assert_uint_eq(ctx.on_match_calls, 1);
    ck_assert_uint_eq(ctx.on_deal_calls, 1);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);
    ck_assert_uint_eq(ctx.on_filled_calls, 1);
    ck_assert_uint_eq(ctx.on_cancel_calls, 1);

    ck_assert(engine.callbacks.pre_booked(NULL, engine.callbacks.user_ctx));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_pre_booked_cancel)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = false;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *taker = make_order(&engine, 10000, 10, OM_SIDE_BID | OM_TYPE_LIMIT);

    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, taker->order_id));
    ck_assert_uint_eq(ctx.on_cancel_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_full_fill_single)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 1);
    ck_assert_uint_eq(ctx.on_match_calls, 2);
    ck_assert_uint_eq(ctx.on_filled_calls, 1);
    ck_assert_uint_eq(ctx.on_booked_calls, 0);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_partial_fill_maker_remaining)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 5, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    OmSlabSlot *maker_left = om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id);
    ck_assert_ptr_nonnull(maker_left);
    ck_assert_uint_eq(maker_left->volume_remain, 5);
    ck_assert_uint_eq(ctx.on_filled_calls, 0);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_partial_fill_taker_booked)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_booked_calls, 1);
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, taker->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_price_not_cross)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10050, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10000, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_multi_maker_levels)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker1 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    OmSlabSlot *maker2 = make_order(&engine, 10100, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker1), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker2), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 2);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker1->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker2->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_same_price_fifo)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker1 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    OmSlabSlot *maker2 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker1), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker2), 0);

    OmSlabSlot *taker = make_order(&engine, 10000, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker1->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker2->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_can_match_cap)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    ctx.can_match_cap = 3;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 3, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    OmSlabSlot *maker_left = om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id);
    ck_assert_ptr_nonnull(maker_left);
    ck_assert_uint_eq(maker_left->volume_remain, 7);
    ck_assert_uint_eq(ctx.on_deal_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_can_match_zero)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    ctx.can_match_zero = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 10, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_can_match_skip_best)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker1 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    OmSlabSlot *maker2 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker1), 0);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker2), 0);

    OmSlabSlot *taker = make_order(&engine, 10000, 5, OM_SIDE_BID | OM_TYPE_LIMIT);

    /* First maker skipped, second allowed */
    ctx.can_match_skip_once = true;
    ctx.can_match_calls = 0;

    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 1);
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, maker1->order_id));
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, maker2->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_can_match_skip_level_then_book)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker1 = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker1), 0);

    OmSlabSlot *maker2 = make_order(&engine, 10100, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker2), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 5, OM_SIDE_BID | OM_TYPE_LIMIT);

    ctx.can_match_zero = true;
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_pre_booked_false_cancels_remaining)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = false;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 10, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_cancel_calls, 1);
    ck_assert_ptr_null(om_orderbook_get_slot_by_id(&engine.orderbook, taker->order_id));

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_multi_product_isolated)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 1, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10100, 5, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_match_bid_vs_bid_no_cross)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    OmSlabSlot *taker = make_order(&engine, 10000, 5, OM_SIDE_BID | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_engine_match(&engine, 0, taker), 0);

    ck_assert_uint_eq(ctx.on_deal_calls, 0);
    ck_assert_uint_eq(ctx.on_booked_calls, 1);

    om_engine_destroy(&engine);
}
END_TEST

START_TEST(test_engine_deactivate_activate)
{
    OmEngine engine;
    TestMatchCtx ctx = {0};
    ctx.pre_booked_allow = true;
    init_engine_with_ctx(&engine, &ctx);

    OmSlabSlot *maker = make_order(&engine, 10000, 5, OM_SIDE_ASK | OM_TYPE_LIMIT);
    ck_assert_int_eq(om_orderbook_insert(&engine.orderbook, 0, maker), 0);

    ck_assert(om_engine_deactivate(&engine, maker->order_id));
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id));
    ck_assert_uint_eq((maker->flags & OM_STATUS_MASK), OM_STATUS_DEACTIVATED);

    ck_assert(om_engine_activate(&engine, maker->order_id));
    ck_assert_ptr_nonnull(om_orderbook_get_slot_by_id(&engine.orderbook, maker->order_id));

    om_engine_destroy(&engine);
}
END_TEST

Suite *engine_suite(void)
{
    Suite *s = suite_create("Engine");
    TCase *tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_engine_init_callbacks);
    tcase_add_test(tc_core, test_engine_callback_context);
    tcase_add_test(tc_core, test_engine_match_pre_booked_cancel);
    tcase_add_test(tc_core, test_engine_match_full_fill_single);
    tcase_add_test(tc_core, test_engine_match_partial_fill_maker_remaining);
    tcase_add_test(tc_core, test_engine_match_partial_fill_taker_booked);
    tcase_add_test(tc_core, test_engine_match_price_not_cross);
    tcase_add_test(tc_core, test_engine_match_multi_maker_levels);
    tcase_add_test(tc_core, test_engine_match_same_price_fifo);
    tcase_add_test(tc_core, test_engine_match_can_match_cap);
    tcase_add_test(tc_core, test_engine_match_can_match_zero);
    tcase_add_test(tc_core, test_engine_match_can_match_skip_best);
    tcase_add_test(tc_core, test_engine_match_can_match_skip_level_then_book);
    tcase_add_test(tc_core, test_engine_match_pre_booked_false_cancels_remaining);
    tcase_add_test(tc_core, test_engine_match_multi_product_isolated);
    tcase_add_test(tc_core, test_engine_match_bid_vs_bid_no_cross);
    tcase_add_test(tc_core, test_engine_deactivate_activate);

    suite_add_tcase(s, tc_core);
    return s;
}
