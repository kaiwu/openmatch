#include "openmarket/om_market.h"
#include <stdlib.h>
#include <string.h>

static void om_market_worker_destroy(OmMarketWorker *worker);
static void om_market_public_worker_destroy(OmMarketPublicWorker *worker);

OmMarketVersion om_market_version(void) {
    OmMarketVersion version = {
        .major = OM_MARKET_VERSION_MAJOR,
        .minor = OM_MARKET_VERSION_MINOR,
        .patch = OM_MARKET_VERSION_PATCH
    };
    return version;
}

const char *om_market_version_string(void) {
    return "1.0.0";
}

static uint32_t om_market_pair_key(uint16_t org_id, uint16_t product_id) {
    return ((uint32_t)org_id << 16) | (uint32_t)product_id;
}

static int om_market_worker_init(OmMarketWorker *worker,
                                 uint32_t worker_id,
                                 uint16_t max_products,
                                 const OmMarketSubscription *subs,
                                 uint32_t sub_count,
                                 size_t expected_orders,
                                 size_t expected_price_levels,
                                 OmMarketDealableFn dealable,
                                 void *dealable_ctx) {
    memset(worker, 0, sizeof(*worker));
    worker->worker_id = worker_id;
    worker->max_products = max_products;
    worker->subscription_count = sub_count;
    worker->dealable = dealable;
    worker->dealable_ctx = dealable_ctx;
    worker->top_levels = 0;

    worker->product_offsets = calloc((size_t)max_products + 1U, sizeof(*worker->product_offsets));
    if (!worker->product_offsets) {
        return -1;
    }
    for (uint32_t i = 0; i < sub_count; i++) {
        if (subs[i].product_id < max_products) {
            worker->product_offsets[subs[i].product_id + 1U]++;
        }
    }
    for (uint32_t i = 1; i <= max_products; i++) {
        worker->product_offsets[i] += worker->product_offsets[i - 1U];
    }

    worker->product_orgs = calloc(sub_count, sizeof(*worker->product_orgs));
    if (!worker->product_orgs) {
        om_market_worker_destroy(worker);
        return -2;
    }

    worker->product_has_subs = calloc((size_t)max_products, sizeof(*worker->product_has_subs));
    if (!worker->product_has_subs) {
        om_market_worker_destroy(worker);
        return -3;
    }

    worker->org_ids = calloc(sub_count, sizeof(*worker->org_ids));
    if (!worker->org_ids) {
        om_market_worker_destroy(worker);
        return -4;
    }
    worker->org_index_map = calloc((size_t)UINT16_MAX + 1U, sizeof(*worker->org_index_map));
    if (!worker->org_index_map) {
        om_market_worker_destroy(worker);
        return -5;
    }
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        worker->org_index_map[i] = UINT32_MAX;
    }

    uint32_t *cursor = calloc((size_t)max_products, sizeof(*cursor));
    if (!cursor) {
        om_market_worker_destroy(worker);
        return -5;
    }
    memcpy(cursor, worker->product_offsets, sizeof(*cursor) * max_products);
    for (uint32_t i = 0; i < sub_count; i++) {
        if (subs[i].product_id >= max_products) {
            continue;
        }
        uint32_t idx = cursor[subs[i].product_id]++;
        worker->product_orgs[idx] = subs[i].org_id;
        worker->product_has_subs[subs[i].product_id] = 1U;
        if (worker->org_index_map[subs[i].org_id] == UINT32_MAX) {
            worker->org_index_map[subs[i].org_id] = worker->org_count;
            worker->org_ids[worker->org_count] = subs[i].org_id;
            worker->org_count++;
        }
    }
    free(cursor);

    worker->ladders = calloc(sub_count, sizeof(*worker->ladders));
    if (!worker->ladders) {
        om_market_worker_destroy(worker);
        return -6;
    }
    worker->ladder_dirty = calloc(sub_count, sizeof(*worker->ladder_dirty));
    if (!worker->ladder_dirty) {
        om_market_worker_destroy(worker);
        return -7;
    }
    worker->ladder_deltas = calloc(sub_count * 2U, sizeof(*worker->ladder_deltas));
    if (!worker->ladder_deltas) {
        om_market_worker_destroy(worker);
        return -8;
    }

    worker->pair_to_ladder = kh_init(om_market_pair_map);
    if (!worker->pair_to_ladder) {
        om_market_worker_destroy(worker);
        return -12;
    }
    for (uint32_t i = 0; i < sub_count; i++) {
        uint32_t key = om_market_pair_key(subs[i].org_id, subs[i].product_id);
        int ret = 0;
        khiter_t it = kh_put(om_market_pair_map, worker->pair_to_ladder, key, &ret);
        if (ret < 0) {
            om_market_worker_destroy(worker);
            return -6;
        }
        kh_val(worker->pair_to_ladder, it) = i;
        worker->ladders[i].bid = kh_init(om_market_price_map);
        worker->ladders[i].ask = kh_init(om_market_price_map);
        if (!worker->ladders[i].bid || !worker->ladders[i].ask) {
            om_market_worker_destroy(worker);
            return -6;
        }
        if (expected_price_levels > 0) {
            kh_resize(om_market_price_map, worker->ladders[i].bid, expected_price_levels);
            kh_resize(om_market_price_map, worker->ladders[i].ask, expected_price_levels);
        }
    }

    worker->orders = calloc(worker->org_count, sizeof(*worker->orders));
    if (!worker->orders) {
        om_market_worker_destroy(worker);
        return -13;
    }
    for (uint32_t i = 0; i < worker->org_count; i++) {
        worker->orders[i] = kh_init(om_market_order_map);
        if (!worker->orders[i]) {
            om_market_worker_destroy(worker);
            return -14;
        }
        if (expected_orders > 0) {
            kh_resize(om_market_order_map, worker->orders[i], expected_orders);
        }
    }


    worker->ladder_index_stride = (size_t)max_products;
    size_t ladder_index_size = (size_t)worker->org_count * worker->ladder_index_stride;
    worker->ladder_index = calloc(ladder_index_size, sizeof(*worker->ladder_index));
    if (!worker->ladder_index) {
        om_market_worker_destroy(worker);
        return -14;
    }
    for (size_t i = 0; i < ladder_index_size; i++) {
        worker->ladder_index[i] = UINT32_MAX;
    }

    for (uint32_t i = 0; i < sub_count; i++) {
        uint32_t org_index = worker->org_index_map[subs[i].org_id];
        if (org_index == UINT32_MAX) {
            continue;
        }
        worker->ladder_index[(size_t)org_index * worker->ladder_index_stride + subs[i].product_id] = i;
    }

    

    return 0;
}

static int om_market_public_worker_init(OmMarketPublicWorker *worker,
                                        uint16_t max_products,
                                        uint32_t top_levels,
                                        size_t expected_orders,
                                        size_t expected_price_levels) {
    memset(worker, 0, sizeof(*worker));
    worker->max_products = max_products;
    worker->top_levels = top_levels;

    worker->product_has_subs = calloc((size_t)max_products, sizeof(*worker->product_has_subs));
    if (!worker->product_has_subs) {
        return -1;
    }
    worker->ladders = calloc((size_t)max_products, sizeof(*worker->ladders));
    if (!worker->ladders) {
        om_market_public_worker_destroy(worker);
        return -2;
    }
    worker->dirty = calloc((size_t)max_products, sizeof(*worker->dirty));
    if (!worker->dirty) {
        om_market_public_worker_destroy(worker);
        return -3;
    }
    worker->deltas = calloc((size_t)max_products * 2U, sizeof(*worker->deltas));
    if (!worker->deltas) {
        om_market_public_worker_destroy(worker);
        return -4;
    }
    worker->orders = kh_init(om_market_order_map);
    if (!worker->orders) {
        om_market_public_worker_destroy(worker);
        return -5;
    }
    if (expected_orders > 0) {
        kh_resize(om_market_order_map, worker->orders, expected_orders);
    }
    (void)expected_price_levels;
    return 0;
}

static void om_market_public_worker_destroy(OmMarketPublicWorker *worker) {
    if (!worker) {
        return;
    }
    if (worker->ladders) {
        for (uint32_t i = 0; i < worker->max_products; i++) {
            if (worker->ladders[i].bid) {
                kh_destroy(om_market_price_map, worker->ladders[i].bid);
            }
            if (worker->ladders[i].ask) {
                kh_destroy(om_market_price_map, worker->ladders[i].ask);
            }
        }
    }
    if (worker->deltas) {
        for (uint32_t i = 0; i < worker->max_products * 2U; i++) {
            if (worker->deltas[i]) {
                kh_destroy(om_market_delta_map, worker->deltas[i]);
            }
        }
    }
    if (worker->orders) {
        kh_destroy(om_market_order_map, worker->orders);
    }
    free(worker->deltas);
    free(worker->dirty);
    free(worker->ladders);
    free(worker->product_has_subs);
    memset(worker, 0, sizeof(*worker));
}

static void om_market_worker_destroy(OmMarketWorker *worker) {
    if (!worker) {
        return;
    }
    if (worker->ladders) {
        for (uint32_t i = 0; i < worker->subscription_count; i++) {
            if (worker->ladders[i].bid) {
                kh_destroy(om_market_price_map, worker->ladders[i].bid);
            }
            if (worker->ladders[i].ask) {
                kh_destroy(om_market_price_map, worker->ladders[i].ask);
            }
        }
    }
    if (worker->pair_to_ladder) {
        kh_destroy(om_market_pair_map, worker->pair_to_ladder);
    }
    if (worker->orders) {
        for (uint32_t i = 0; i < worker->org_count; i++) {
            if (worker->orders[i]) {
                kh_destroy(om_market_order_map, worker->orders[i]);
            }
        }
        free(worker->orders);
    }
    free(worker->ladder_index);
    free(worker->ladder_dirty);
    if (worker->ladder_deltas) {
        for (uint32_t i = 0; i < worker->subscription_count * 2U; i++) {
            if (worker->ladder_deltas[i]) {
                kh_destroy(om_market_delta_map, worker->ladder_deltas[i]);
            }
        }
    }
    free(worker->product_has_subs);
    free(worker->ladder_deltas);
    free(worker->product_offsets);
    free(worker->product_orgs);
    free(worker->org_ids);
    free(worker->org_index_map);
    free(worker->ladders);
    memset(worker, 0, sizeof(*worker));
}

int om_market_init(OmMarket *market, const OmMarketConfig *config) {
    if (!market || !config || !config->org_to_worker || !config->subs || config->sub_count == 0) {
        return -1;
    }
    if (config->worker_count == 0 || config->max_products == 0) {
        return -2;
    }
    if (!config->dealable) {
        return -3;
    }

    memset(market, 0, sizeof(*market));
    market->worker_count = config->worker_count;
    market->public_worker_count = config->public_worker_count;
    market->max_products = config->max_products;
    market->top_levels = config->top_levels;
    market->dealable = config->dealable;
    market->dealable_ctx = config->dealable_ctx;

    market->workers = calloc(config->worker_count, sizeof(*market->workers));
    if (!market->workers) {
        return -4;
    }
    market->public_workers = calloc(config->public_worker_count, sizeof(*market->public_workers));
    if (!market->public_workers) {
        free(market->workers);
        market->workers = NULL;
        return -5;
    }

    uint32_t *counts = calloc(config->worker_count, sizeof(*counts));
    if (!counts) {
        free(market->public_workers);
        free(market->workers);
        market->public_workers = NULL;
        market->workers = NULL;
        return -6;
    }
    for (uint32_t i = 0; i < config->sub_count; i++) {
        uint16_t org_id = config->subs[i].org_id;
        uint32_t worker_id = config->org_to_worker[org_id];
        if (worker_id >= config->worker_count) {
            free(counts);
            free(market->public_workers);
            free(market->workers);
            market->public_workers = NULL;
            market->workers = NULL;
            return -7;
        }
        counts[worker_id]++;
    }

    OmMarketSubscription *buckets = calloc(config->sub_count, sizeof(*buckets));
    if (!buckets) {
        free(counts);
        free(market->public_workers);
        free(market->workers);
        market->public_workers = NULL;
        market->workers = NULL;
        return -8;
    }
    uint32_t *offsets = calloc(config->worker_count, sizeof(*offsets));
    if (!offsets) {
        free(buckets);
        free(counts);
        free(market->public_workers);
        free(market->workers);
        market->public_workers = NULL;
        market->workers = NULL;
        return -9;
    }
    uint32_t total = 0;
    for (uint32_t w = 0; w < config->worker_count; w++) {
        offsets[w] = total;
        total += counts[w];
    }
    for (uint32_t i = 0; i < config->sub_count; i++) {
        uint16_t org_id = config->subs[i].org_id;
        uint32_t worker_id = config->org_to_worker[org_id];
        buckets[offsets[worker_id]++] = config->subs[i];
    }

    total = 0;
    for (uint32_t w = 0; w < config->worker_count; w++) {
        uint32_t count = counts[w];
        int ret = om_market_worker_init(&market->workers[w], w, config->max_products,
                                        buckets + total, count,
                                        config->expected_orders_per_worker,
                                        config->expected_price_levels,
                                        config->dealable,
                                        config->dealable_ctx);
        if (ret != 0) {
            free(buckets);
            free(offsets);
            free(counts);
            om_market_destroy(market);
            return ret;
        }
        market->workers[w].top_levels = config->top_levels;
        total += count;
    }

    if (!config->product_to_public_worker || config->public_worker_count == 0) {
        free(buckets);
        free(offsets);
        free(counts);
        om_market_destroy(market);
        return -10;
    }
    for (uint32_t w = 0; w < config->public_worker_count; w++) {
        int ret = om_market_public_worker_init(&market->public_workers[w], config->max_products,
                                               config->top_levels,
                                               config->expected_orders_per_worker,
                                               config->expected_price_levels);
        if (ret != 0) {
            free(buckets);
            free(offsets);
            free(counts);
            om_market_destroy(market);
            return ret;
        }
    }

    uint8_t *public_products = calloc((size_t)config->max_products, sizeof(*public_products));
    if (!public_products) {
        free(buckets);
        free(offsets);
        free(counts);
        om_market_destroy(market);
        return -11;
    }
    for (uint32_t i = 0; i < config->sub_count; i++) {
        uint16_t product_id = config->subs[i].product_id;
        if (product_id < config->max_products) {
            public_products[product_id] = 1U;
        }
    }
    for (uint32_t product_id = 0; product_id < config->max_products; product_id++) {
        if (!public_products[product_id]) {
            continue;
        }
        uint32_t worker_id = config->product_to_public_worker[product_id];
        if (worker_id >= config->public_worker_count) {
            free(public_products);
            free(buckets);
            free(offsets);
            free(counts);
            om_market_destroy(market);
            return -12;
        }
        market->public_workers[worker_id].product_has_subs[product_id] = 1U;
        if (!market->public_workers[worker_id].ladders[product_id].bid &&
            !market->public_workers[worker_id].ladders[product_id].ask) {
            market->public_workers[worker_id].ladders[product_id].bid = kh_init(om_market_price_map);
            market->public_workers[worker_id].ladders[product_id].ask = kh_init(om_market_price_map);
            if (!market->public_workers[worker_id].ladders[product_id].bid ||
                !market->public_workers[worker_id].ladders[product_id].ask) {
                free(public_products);
                free(buckets);
                free(offsets);
                free(counts);
                om_market_destroy(market);
                return -13;
            }
            if (config->expected_price_levels > 0) {
                kh_resize(om_market_price_map,
                          market->public_workers[worker_id].ladders[product_id].bid,
                          config->expected_price_levels);
                kh_resize(om_market_price_map,
                          market->public_workers[worker_id].ladders[product_id].ask,
                          config->expected_price_levels);
            }
        }
    }
    free(public_products);

    free(buckets);
    free(offsets);
    free(counts);
    return 0;
}

void om_market_destroy(OmMarket *market) {
    if (!market) {
        return;
    }
    for (uint32_t i = 0; i < market->worker_count; i++) {
        om_market_worker_destroy(&market->workers[i]);
    }
    for (uint32_t i = 0; i < market->public_worker_count; i++) {
        om_market_public_worker_destroy(&market->public_workers[i]);
    }
    free(market->workers);
    free(market->public_workers);
    memset(market, 0, sizeof(*market));
}

OmMarketWorker *om_market_worker(OmMarket *market, uint32_t worker_id) {
    if (!market || worker_id >= market->worker_count) {
        return NULL;
    }
    return &market->workers[worker_id];
}

static int om_market_worker_find_ladder(const OmMarketWorker *worker,
                                        uint16_t org_id,
                                        uint16_t product_id,
                                        uint32_t *out_index) {
    if (!worker || !out_index) {
        return -1;
    }
    uint32_t org_index = worker->org_index_map[org_id];
    if (org_index == UINT32_MAX) {
        return -2;
    }
    size_t idx = (size_t)org_index * worker->ladder_index_stride + product_id;
    uint32_t ladder_idx = worker->ladder_index[idx];
    if (ladder_idx == UINT32_MAX) {
        return -2;
    }
    *out_index = ladder_idx;
    return 0;
}

static int om_market_ladder_apply(khash_t(om_market_price_map) *map,
                                  uint64_t price,
                                  uint64_t delta,
                                  bool add) {
    if (!map || delta == 0) {
        return 0;
    }
    int ret = 0;
    khiter_t it = kh_get(om_market_price_map, map, price);
    if (it == kh_end(map)) {
        if (!add) {
            return 0;
        }
        it = kh_put(om_market_price_map, map, price, &ret);
        if (ret < 0) {
            return -1;
        }
        kh_val(map, it) = delta;
        return 0;
    }

    uint64_t value = kh_val(map, it);
    if (add) {
        kh_val(map, it) = value + delta;
        return 0;
    }

    if (value <= delta) {
        kh_del(om_market_price_map, map, it);
        return 0;
    }
    kh_val(map, it) = value - delta;
    return 0;
}

static void om_market_ladder_mark_dirty(OmMarketWorker *worker, uint32_t ladder_idx) {
    if (worker && worker->ladder_dirty && ladder_idx < worker->subscription_count) {
        worker->ladder_dirty[ladder_idx] = 1U;
    }
}

static void om_market_public_mark_dirty(OmMarketPublicWorker *worker, uint16_t product_id) {
    if (worker && worker->dirty && product_id < worker->max_products) {
        worker->dirty[product_id] = 1U;
    }
}

static khash_t(om_market_delta_map) *om_market_delta_for_ladder(OmMarketWorker *worker,
                                                                uint32_t ladder_idx,
                                                                bool bids) {
    if (!worker || !worker->ladder_deltas || ladder_idx >= worker->subscription_count) {
        return NULL;
    }
    uint32_t idx = ladder_idx * 2U + (bids ? 0U : 1U);
    if (!worker->ladder_deltas[idx]) {
        worker->ladder_deltas[idx] = kh_init(om_market_delta_map);
    }
    return worker->ladder_deltas[idx];
}

static khash_t(om_market_delta_map) *om_market_delta_for_public(OmMarketPublicWorker *worker,
                                                                uint16_t product_id,
                                                                bool bids) {
    if (!worker || !worker->deltas || product_id >= worker->max_products) {
        return NULL;
    }
    uint32_t idx = (uint32_t)product_id * 2U + (bids ? 0U : 1U);
    if (!worker->deltas[idx]) {
        worker->deltas[idx] = kh_init(om_market_delta_map);
    }
    return worker->deltas[idx];
}

static void om_market_delta_add(khash_t(om_market_delta_map) *map,
                                uint64_t price,
                                int64_t delta) {
    if (!map || delta == 0) {
        return;
    }
    int ret = 0;
    khiter_t it = kh_get(om_market_delta_map, map, price);
    if (it == kh_end(map)) {
        it = kh_put(om_market_delta_map, map, price, &ret);
        if (ret < 0) {
            return;
        }
        kh_val(map, it) = delta;
        return;
    }
    kh_val(map, it) += delta;
    if (kh_val(map, it) == 0) {
        kh_del(om_market_delta_map, map, it);
    }
}

static uint32_t om_market_best_count(khash_t(om_market_price_map) *map) {
    if (!map) {
        return 0;
    }
    return (uint32_t)kh_size(map);
}

static bool om_market_price_in_top(khash_t(om_market_price_map) *map,
                                   uint64_t price,
                                   uint32_t top_levels,
                                   bool bids) {
    if (!map || top_levels == 0) {
        return false;
    }
    uint32_t better = 0;
    for (khiter_t it = kh_begin(map); it != kh_end(map); ++it) {
        if (!kh_exist(map, it)) {
            continue;
        }
        uint64_t level = kh_key(map, it);
        if (bids) {
            if (level > price) {
                better++;
            }
        } else {
            if (level < price) {
                better++;
            }
        }
        if (better >= top_levels) {
            return false;
        }
    }
    return true;
}

static void om_market_trim_to_top(khash_t(om_market_price_map) *map,
                                  uint32_t top_levels,
                                  bool bids) {
    if (!map || top_levels == 0) {
        return;
    }
    while (om_market_best_count(map) > top_levels) {
        khiter_t worst_it = kh_end(map);
        uint64_t worst_price = 0;
        bool first = true;
        for (khiter_t it = kh_begin(map); it != kh_end(map); ++it) {
            if (!kh_exist(map, it)) {
                continue;
            }
            uint64_t level = kh_key(map, it);
            if (first) {
                worst_it = it;
                worst_price = level;
                first = false;
                continue;
            }
            if (bids) {
                if (level < worst_price) {
                    worst_price = level;
                    worst_it = it;
                }
            } else {
                if (level > worst_price) {
                    worst_price = level;
                    worst_it = it;
                }
            }
        }
        if (worst_it == kh_end(map)) {
            break;
        }
        kh_del(om_market_price_map, map, worst_it);
    }
}

static int om_market_ladder_apply_top(khash_t(om_market_price_map) *map,
                                      uint64_t price,
                                      uint64_t delta,
                                      bool add,
                                      uint32_t top_levels,
                                      bool bids) {
    if (!map || delta == 0) {
        return 0;
    }
    if (top_levels == 0) {
        return 0;
    }
    if (add) {
        if (!om_market_price_in_top(map, price, top_levels, bids)) {
            return 0;
        }
        int ret = om_market_ladder_apply(map, price, delta, true);
        if (ret != 0) {
            return ret;
        }
        om_market_trim_to_top(map, top_levels, bids);
        return 0;
    }
    return om_market_ladder_apply(map, price, delta, false);
}

static int om_market_worker_record_order(OmMarketWorker *worker,
                                         uint32_t org_index,
                                         const OmWalInsert *rec,
                                         OmMarketOrderState *state) {
    if (!worker || !rec || !state) {
        return -1;
    }
    int ret = 0;
    khiter_t it = kh_put(om_market_order_map, worker->orders[org_index], rec->order_id, &ret);
    if (ret < 0) {
        return -2;
    }
    *state = (OmMarketOrderState){
        .product_id = rec->product_id,
        .side = OM_GET_SIDE(rec->flags),
        .active = true,
        .price = rec->price,
        .remaining = rec->vol_remain
    };
    kh_val(worker->orders[org_index], it) = *state;

    return 0;
}

static OmMarketOrderState *om_market_worker_lookup(OmMarketWorker *worker,
                                                   uint32_t org_index,
                                                   uint64_t order_id) {
    if (!worker) {
        return NULL;
    }
    khiter_t it = kh_get(om_market_order_map, worker->orders[org_index], order_id);
    if (it == kh_end(worker->orders[org_index])) {
        return NULL;
    }
    return &kh_val(worker->orders[org_index], it);
}

int om_market_worker_process(OmMarketWorker *worker, OmWalType type, const void *data) {
    if (!worker || !data) {
        return -1;
    }

    switch (type) {
        case OM_WAL_INSERT: {
            const OmWalInsert *rec = (const OmWalInsert *)data;
            if (worker->product_has_subs && !worker->product_has_subs[rec->product_id]) {
                return 0;
            }
            uint32_t start = worker->product_offsets[rec->product_id];
            uint32_t end = worker->product_offsets[rec->product_id + 1U];
            for (uint32_t idx = start; idx < end; idx++) {
                uint16_t viewer_org = worker->product_orgs[idx];
                uint32_t ladder_idx = 0;
                if (om_market_worker_find_ladder(worker, viewer_org, rec->product_id, &ladder_idx) != 0) {
                    continue;
                }
                uint32_t org_index = worker->org_index_map[viewer_org];
                if (org_index == UINT32_MAX) {
                    continue;
                }
                uint64_t dealable_qty = worker->dealable ? worker->dealable(rec, viewer_org,
                                                                              worker->dealable_ctx)
                                                         : 0;
                if (dealable_qty == 0) {
                    continue;
                }
                uint64_t qty = rec->vol_remain < dealable_qty ? rec->vol_remain : dealable_qty;
                OmMarketOrderState state;
                if (om_market_worker_record_order(worker, org_index, rec, &state) != 0) {
                    return -3;
                }
                khiter_t update_it = kh_get(om_market_order_map, worker->orders[org_index], rec->order_id);
                if (update_it != kh_end(worker->orders[org_index])) {
                    kh_val(worker->orders[org_index], update_it).remaining = qty;
                }
                khash_t(om_market_price_map) *map = OM_IS_BID(rec->flags)
                                                       ? worker->ladders[ladder_idx].bid
                                                       : worker->ladders[ladder_idx].ask;
                int ret = om_market_ladder_apply_top(map, rec->price, qty, true,
                                                    worker->top_levels, OM_IS_BID(rec->flags));
                if (ret != 0) {
                    return ret;
                }
                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, OM_IS_BID(rec->flags));
                om_market_delta_add(delta_map, rec->price, (int64_t)qty);
                om_market_ladder_mark_dirty(worker, ladder_idx);
            }
            return 0;
        }
        case OM_WAL_CANCEL:
        case OM_WAL_DEACTIVATE: {
            const OmWalCancel *rec = (const OmWalCancel *)data;
            for (uint32_t i = 0; i < worker->org_count; i++) {
                OmMarketOrderState *state = om_market_worker_lookup(worker, i, rec->order_id);
                if (!state || !state->active) {
                    continue;
                }
                uint16_t org_id = worker->org_ids[i];
                uint32_t ladder_idx = 0;
                if (om_market_worker_find_ladder(worker, org_id, state->product_id, &ladder_idx) != 0) {
                    continue;
                }
                khash_t(om_market_price_map) *map = state->side == OM_SIDE_BID
                                                       ? worker->ladders[ladder_idx].bid
                                                       : worker->ladders[ladder_idx].ask;
                int ret = om_market_ladder_apply_top(map, state->price, state->remaining, false,
                                                    worker->top_levels, state->side == OM_SIDE_BID);
                uint64_t removed = state->remaining;
                state->remaining = 0;
                state->active = false;
                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, state->side == OM_SIDE_BID);
                om_market_delta_add(delta_map, state->price, -(int64_t)removed);
                om_market_ladder_mark_dirty(worker, ladder_idx);
                return ret;
            }
            return 0;
        }
        case OM_WAL_ACTIVATE: {
            const OmWalActivate *rec = (const OmWalActivate *)data;
            for (uint32_t i = 0; i < worker->org_count; i++) {
                OmMarketOrderState *state = om_market_worker_lookup(worker, i, rec->order_id);
                if (!state || state->active || state->remaining == 0) {
                    continue;
                }
                uint16_t org_id = worker->org_ids[i];
                uint32_t ladder_idx = 0;
                if (om_market_worker_find_ladder(worker, org_id, state->product_id, &ladder_idx) != 0) {
                    continue;
                }
                khash_t(om_market_price_map) *map = state->side == OM_SIDE_BID
                                                       ? worker->ladders[ladder_idx].bid
                                                       : worker->ladders[ladder_idx].ask;
                uint64_t added = state->remaining;
                state->active = true;
                int ret = om_market_ladder_apply_top(map, state->price, added, true,
                                                    worker->top_levels, state->side == OM_SIDE_BID);
                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, state->side == OM_SIDE_BID);
                om_market_delta_add(delta_map, state->price, (int64_t)added);
                om_market_ladder_mark_dirty(worker, ladder_idx);
                return ret;
            }
            return 0;
        }
        case OM_WAL_MATCH: {
            const OmWalMatch *rec = (const OmWalMatch *)data;
            for (uint32_t i = 0; i < worker->org_count; i++) {
                OmMarketOrderState *maker = om_market_worker_lookup(worker, i, rec->maker_id);
                if (maker && maker->active && maker->remaining > 0) {
                    uint16_t org_id = worker->org_ids[i];
                    uint32_t ladder_idx = 0;
                    if (om_market_worker_find_ladder(worker, org_id, maker->product_id,
                                                     &ladder_idx) == 0) {
                        khash_t(om_market_price_map) *map = maker->side == OM_SIDE_BID
                                                               ? worker->ladders[ladder_idx].bid
                                                               : worker->ladders[ladder_idx].ask;
                        uint64_t delta = rec->volume > maker->remaining ? maker->remaining : rec->volume;
                        int ret = om_market_ladder_apply_top(map, maker->price, delta, false,
                                                            worker->top_levels, maker->side == OM_SIDE_BID);
                        maker->remaining -= delta;
                        if (ret != 0) {
                            return ret;
                        }
                        khash_t(om_market_delta_map) *delta_map =
                            om_market_delta_for_ladder(worker, ladder_idx, maker->side == OM_SIDE_BID);
                        om_market_delta_add(delta_map, maker->price, -(int64_t)delta);
                        om_market_ladder_mark_dirty(worker, ladder_idx);
                    }
                }
            }
            return 0;
        }
        default:
            return 0;
    }
}

int om_market_public_process(OmMarketPublicWorker *worker, OmWalType type, const void *data) {
    if (!worker || !data) {
        return -1;
    }

    switch (type) {
        case OM_WAL_INSERT: {
            const OmWalInsert *rec = (const OmWalInsert *)data;
            if (worker->product_has_subs && !worker->product_has_subs[rec->product_id]) {
                return 0;
            }
            int pub_ret = 0;
            khiter_t pub_it = kh_put(om_market_order_map, worker->orders, rec->order_id, &pub_ret);
            if (pub_ret < 0) {
                return -2;
            }
            OmMarketOrderState pub_state = {
                .product_id = rec->product_id,
                .side = OM_GET_SIDE(rec->flags),
                .active = true,
                .price = rec->price,
                .remaining = rec->vol_remain
            };
            kh_val(worker->orders, pub_it) = pub_state;
            OmMarketLadder *pub_ladder = &worker->ladders[rec->product_id];
            khash_t(om_market_price_map) *pub_map = OM_IS_BID(rec->flags)
                                                     ? pub_ladder->bid
                                                     : pub_ladder->ask;
            int ret = om_market_ladder_apply_top(pub_map, rec->price, rec->vol_remain, true,
                                                worker->top_levels, OM_IS_BID(rec->flags));
            if (ret != 0) {
                return ret;
            }
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, rec->product_id, OM_IS_BID(rec->flags));
            om_market_delta_add(delta_map, rec->price, (int64_t)rec->vol_remain);
            om_market_public_mark_dirty(worker, rec->product_id);
            return 0;
        }
        case OM_WAL_CANCEL:
        case OM_WAL_DEACTIVATE: {
            const OmWalCancel *rec = (const OmWalCancel *)data;
            khiter_t pub_it = kh_get(om_market_order_map, worker->orders, rec->order_id);
            if (pub_it == kh_end(worker->orders)) {
                return 0;
            }
            OmMarketOrderState *pub_state = &kh_val(worker->orders, pub_it);
            if (!pub_state->active || pub_state->remaining == 0) {
                return 0;
            }
            OmMarketLadder *pub_ladder = &worker->ladders[pub_state->product_id];
            khash_t(om_market_price_map) *pub_map = pub_state->side == OM_SIDE_BID
                                                     ? pub_ladder->bid
                                                     : pub_ladder->ask;
            int ret = om_market_ladder_apply_top(pub_map, pub_state->price,
                                                 pub_state->remaining, false,
                                                 worker->top_levels,
                                                 pub_state->side == OM_SIDE_BID);
            uint64_t removed = pub_state->remaining;
            pub_state->remaining = 0;
            pub_state->active = false;
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, pub_state->product_id,
                                           pub_state->side == OM_SIDE_BID);
            om_market_delta_add(delta_map, pub_state->price, -(int64_t)removed);
            om_market_public_mark_dirty(worker, pub_state->product_id);
            return ret;
        }
        case OM_WAL_ACTIVATE: {
            const OmWalActivate *rec = (const OmWalActivate *)data;
            khiter_t pub_it = kh_get(om_market_order_map, worker->orders, rec->order_id);
            if (pub_it == kh_end(worker->orders)) {
                return 0;
            }
            OmMarketOrderState *pub_state = &kh_val(worker->orders, pub_it);
            if (pub_state->active || pub_state->remaining == 0) {
                return 0;
            }
            OmMarketLadder *pub_ladder = &worker->ladders[pub_state->product_id];
            khash_t(om_market_price_map) *pub_map = pub_state->side == OM_SIDE_BID
                                                     ? pub_ladder->bid
                                                     : pub_ladder->ask;
            uint64_t added = pub_state->remaining;
            int ret = om_market_ladder_apply_top(pub_map, pub_state->price,
                                                 added, true,
                                                 worker->top_levels,
                                                 pub_state->side == OM_SIDE_BID);
            pub_state->active = true;
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, pub_state->product_id,
                                           pub_state->side == OM_SIDE_BID);
            om_market_delta_add(delta_map, pub_state->price, (int64_t)added);
            om_market_public_mark_dirty(worker, pub_state->product_id);
            return ret;
        }
        case OM_WAL_MATCH: {
            const OmWalMatch *rec = (const OmWalMatch *)data;
            khiter_t pub_it = kh_get(om_market_order_map, worker->orders, rec->maker_id);
            if (pub_it == kh_end(worker->orders)) {
                return 0;
            }
            OmMarketOrderState *pub_state = &kh_val(worker->orders, pub_it);
            if (!pub_state->active || pub_state->remaining == 0) {
                return 0;
            }
            OmMarketLadder *pub_ladder = &worker->ladders[pub_state->product_id];
            khash_t(om_market_price_map) *pub_map = pub_state->side == OM_SIDE_BID
                                                     ? pub_ladder->bid
                                                     : pub_ladder->ask;
            uint64_t delta = rec->volume > pub_state->remaining
                                 ? pub_state->remaining
                                 : rec->volume;
            int ret = om_market_ladder_apply_top(pub_map, pub_state->price, delta, false,
                                                 worker->top_levels,
                                                 pub_state->side == OM_SIDE_BID);
            pub_state->remaining -= delta;
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, pub_state->product_id,
                                           pub_state->side == OM_SIDE_BID);
            om_market_delta_add(delta_map, pub_state->price, -(int64_t)delta);
            om_market_public_mark_dirty(worker, pub_state->product_id);
            return ret;
        }
        default:
            return 0;
    }
}

int om_market_worker_get_qty(const OmMarketWorker *worker,
                             uint16_t org_id,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty) {
    if (!worker || !out_qty) {
        return -1;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return -1;
    }
    khash_t(om_market_price_map) *map = side == OM_SIDE_BID
                                           ? worker->ladders[ladder_idx].bid
                                           : worker->ladders[ladder_idx].ask;
    khiter_t it = kh_get(om_market_price_map, map, price);
    if (it == kh_end(map)) {
        return -1;
    }
    *out_qty = kh_val(map, it);
    return 0;
}

int om_market_public_get_qty(const OmMarketPublicWorker *worker,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty) {
    if (!worker || !out_qty || !worker->ladders || product_id >= worker->max_products) {
        return -1;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return -1;
    }
    const OmMarketLadder *ladder = &worker->ladders[product_id];
    khash_t(om_market_price_map) *map = side == OM_SIDE_BID ? ladder->bid : ladder->ask;
    if (!map) {
        return -1;
    }
    khiter_t it = kh_get(om_market_price_map, map, price);
    if (it == kh_end(map)) {
        return -1;
    }
    *out_qty = kh_val(map, it);
    return 0;
}

int om_market_worker_is_subscribed(const OmMarketWorker *worker,
                                  uint16_t org_id,
                                  uint16_t product_id) {
    if (!worker) {
        return -1;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return 0;
    }
    return 1;
}

int om_market_worker_delta_count(const OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id,
                                 uint16_t side) {
    if (!worker || !worker->ladder_deltas) {
        return -1;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return -1;
    }
    uint32_t idx = ladder_idx * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->ladder_deltas[idx];
    return map ? (int)kh_size(map) : 0;
}

int om_market_worker_copy_deltas(const OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id,
                                 uint16_t side,
                                 OmMarketDelta *out,
                                 size_t max) {
    if (!worker || !out || max == 0 || !worker->ladder_deltas) {
        return -1;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return -1;
    }
    uint32_t idx = ladder_idx * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->ladder_deltas[idx];
    if (!map) {
        return 0;
    }
    size_t count = 0;
    for (khiter_t it = kh_begin(map); it != kh_end(map) && count < max; ++it) {
        if (!kh_exist(map, it)) {
            continue;
        }
        out[count].price = kh_key(map, it);
        out[count].delta = kh_val(map, it);
        count++;
    }
    return (int)count;
}

int om_market_worker_clear_deltas(OmMarketWorker *worker,
                                  uint16_t org_id,
                                  uint16_t product_id,
                                  uint16_t side) {
    if (!worker || !worker->ladder_deltas) {
        return -1;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return -1;
    }
    uint32_t idx = ladder_idx * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->ladder_deltas[idx];
    if (!map) {
        return 0;
    }
    kh_clear(om_market_delta_map, map);
    return 0;
}

int om_market_public_delta_count(const OmMarketPublicWorker *worker,
                                 uint16_t product_id,
                                 uint16_t side) {
    if (!worker || !worker->deltas || product_id >= worker->max_products) {
        return -1;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return -1;
    }
    uint32_t idx = (uint32_t)product_id * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->deltas[idx];
    return map ? (int)kh_size(map) : 0;
}

int om_market_public_copy_deltas(const OmMarketPublicWorker *worker,
                                 uint16_t product_id,
                                 uint16_t side,
                                 OmMarketDelta *out,
                                 size_t max) {
    if (!worker || !out || max == 0 || !worker->deltas || product_id >= worker->max_products) {
        return -1;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return -1;
    }
    uint32_t idx = (uint32_t)product_id * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->deltas[idx];
    if (!map) {
        return 0;
    }
    size_t count = 0;
    for (khiter_t it = kh_begin(map); it != kh_end(map) && count < max; ++it) {
        if (!kh_exist(map, it)) {
            continue;
        }
        out[count].price = kh_key(map, it);
        out[count].delta = kh_val(map, it);
        count++;
    }
    return (int)count;
}

int om_market_public_clear_deltas(OmMarketPublicWorker *worker,
                                  uint16_t product_id,
                                  uint16_t side) {
    if (!worker || !worker->deltas || product_id >= worker->max_products) {
        return -1;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return -1;
    }
    uint32_t idx = (uint32_t)product_id * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->deltas[idx];
    if (!map) {
        return 0;
    }
    kh_clear(om_market_delta_map, map);
    return 0;
}

int om_market_worker_copy_full(const OmMarketWorker *worker,
                               uint16_t org_id,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max) {
    if (!worker || !out || max == 0) {
        return -1;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return -1;
    }
    khash_t(om_market_price_map) *map = side == OM_SIDE_BID
                                           ? worker->ladders[ladder_idx].bid
                                           : worker->ladders[ladder_idx].ask;
    if (!map) {
        return 0;
    }
    size_t count = 0;
    for (khiter_t it = kh_begin(map); it != kh_end(map) && count < max; ++it) {
        if (!kh_exist(map, it)) {
            continue;
        }
        out[count].price = kh_key(map, it);
        out[count].delta = (int64_t)kh_val(map, it);
        count++;
    }
    return (int)count;
}

int om_market_public_copy_full(const OmMarketPublicWorker *worker,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max) {
    if (!worker || !out || max == 0 || product_id >= worker->max_products) {
        return -1;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return -1;
    }
    const OmMarketLadder *ladder = &worker->ladders[product_id];
    khash_t(om_market_price_map) *map = side == OM_SIDE_BID ? ladder->bid : ladder->ask;
    if (!map) {
        return 0;
    }
    size_t count = 0;
    for (khiter_t it = kh_begin(map); it != kh_end(map) && count < max; ++it) {
        if (!kh_exist(map, it)) {
            continue;
        }
        out[count].price = kh_key(map, it);
        out[count].delta = (int64_t)kh_val(map, it);
        count++;
    }
    return (int)count;
}

int om_market_worker_is_dirty(const OmMarketWorker *worker,
                              uint16_t org_id,
                              uint16_t product_id) {
    if (!worker || !worker->ladder_dirty) {
        return -1;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return -1;
    }
    return worker->ladder_dirty[ladder_idx] ? 1 : 0;
}

int om_market_worker_clear_dirty(OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id) {
    if (!worker || !worker->ladder_dirty) {
        return -1;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return -1;
    }
    worker->ladder_dirty[ladder_idx] = 0;
    return 0;
}

int om_market_public_is_dirty(const OmMarketPublicWorker *worker, uint16_t product_id) {
    if (!worker || !worker->dirty || product_id >= worker->max_products) {
        return -1;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return -1;
    }
    return worker->dirty[product_id] ? 1 : 0;
}

int om_market_public_clear_dirty(OmMarketPublicWorker *worker, uint16_t product_id) {
    if (!worker || !worker->dirty || product_id >= worker->max_products) {
        return -1;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return -1;
    }
    worker->dirty[product_id] = 0;
    return 0;
}
