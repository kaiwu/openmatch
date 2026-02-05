#include "openmarket/om_market.h"
#include "openmatch/om_error.h"
#include <stdlib.h>
#include <string.h>

/* Cache line size for alignment to prevent false sharing */
#define OM_CACHE_LINE_SIZE 64

static void om_market_worker_destroy(OmMarketWorker *worker);
static void om_market_public_worker_destroy(OmMarketPublicWorker *worker);

/* Allocate cache-line aligned memory, zero-initialized */
static void *om_aligned_calloc(size_t count, size_t size) {
    size_t total = count * size;
    if (total == 0) {
        return NULL;
    }
    /* Round up to cache line boundary */
    size_t aligned_size = (total + OM_CACHE_LINE_SIZE - 1) & ~(OM_CACHE_LINE_SIZE - 1);
    void *ptr = aligned_alloc(OM_CACHE_LINE_SIZE, aligned_size);
    if (ptr) {
        memset(ptr, 0, aligned_size);
    }
    return ptr;
}

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

/* ============================================================================
 * Sorted Array Ladder Operations (Cache-Optimized)
 * ============================================================================ */

/* Binary search for price in sorted level array.
 * Returns index where price is found or should be inserted.
 * For bids: sorted descending (best/highest first)
 * For asks: sorted ascending (best/lowest first)
 */
static uint32_t om_ladder_find_pos(const OmMarketLevel *levels, uint32_t count,
                                    uint64_t price, bool is_bid, bool *found) {
    *found = false;
    if (count == 0) {
        return 0;
    }

    uint32_t lo = 0;
    uint32_t hi = count;

    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (levels[mid].price == price) {
            *found = true;
            return mid;
        }
        bool go_left = is_bid ? (levels[mid].price < price) : (levels[mid].price > price);
        if (go_left) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
}

/* Add quantity to ladder at price. Returns 0 on success. */
static int om_ladder_add(OmMarketLevel *levels, uint32_t *count,
                         uint32_t capacity, uint64_t price, uint64_t qty, bool is_bid) {
    if (qty == 0 || capacity == 0) {
        return 0;
    }

    bool found = false;
    uint32_t pos = om_ladder_find_pos(levels, *count, price, is_bid, &found);

    if (found) {
        /* Price exists, just add quantity */
        levels[pos].qty += qty;
        return 0;
    }

    /* Check if this price qualifies for top-N */
    if (pos >= capacity) {
        return 0;  /* Outside top-N, ignore */
    }

    /* Need to insert at position 'pos' */
    uint32_t shift_count = 0;
    if (*count < capacity) {
        /* We have room, shift everything after pos */
        shift_count = *count - pos;
        (*count)++;
    } else {
        /* Full, shift everything after pos but drop the last one */
        shift_count = capacity - 1 - pos;
    }

    if (shift_count > 0) {
        memmove(&levels[pos + 1], &levels[pos], shift_count * sizeof(OmMarketLevel));
    }

    levels[pos].price = price;
    levels[pos].qty = qty;
    return 0;
}

/* Subtract quantity from ladder at price.
 * Returns: 1 if a level was removed entirely, 0 otherwise, negative on error.
 * If out_removed_price is non-NULL and a level was removed, stores the removed price. */
static int om_ladder_sub(OmMarketLevel *levels, uint32_t *count,
                         uint64_t price, uint64_t qty, bool is_bid,
                         uint64_t *out_removed_price) {
    if (qty == 0 || *count == 0) {
        return 0;
    }

    bool found = false;
    uint32_t pos = om_ladder_find_pos(levels, *count, price, is_bid, &found);

    if (!found) {
        return 0;  /* Price not in top-N, nothing to do */
    }

    if (qty >= levels[pos].qty) {
        /* Remove level entirely */
        if (out_removed_price) {
            *out_removed_price = levels[pos].price;
        }
        uint32_t shift_count = *count - pos - 1;
        if (shift_count > 0) {
            memmove(&levels[pos], &levels[pos + 1], shift_count * sizeof(OmMarketLevel));
        }
        (*count)--;
        return 1;  /* Level was removed */
    } else {
        levels[pos].qty -= qty;
    }
    return 0;
}

/* Check if a price is already in the ladder. */
static bool om_ladder_contains_price(const OmMarketLevel *levels, uint32_t count,
                                      uint64_t price, bool is_bid) {
    bool found = false;
    om_ladder_find_pos(levels, count, price, is_bid, &found);
    return found;
}

/* Get the worst (boundary) price in the ladder, or 0 if empty.
 * For bids (descending): worst = last = lowest price
 * For asks (ascending): worst = last = highest price */
static uint64_t om_ladder_worst_price(const OmMarketLevel *levels, uint32_t count) {
    if (count == 0) {
        return 0;
    }
    return levels[count - 1].price;
}

/* Check if candidate price would qualify for top-N (is better than worst or has room).
 * For bids: candidate > worst (higher is better)
 * For asks: candidate < worst (lower is better) */
static bool om_ladder_price_qualifies(const OmMarketLevel *levels, uint32_t count,
                                       uint32_t capacity, uint64_t price, bool is_bid) {
    if (count < capacity) {
        return true;  /* Have room */
    }
    uint64_t worst = om_ladder_worst_price(levels, count);
    return is_bid ? (price > worst) : (price < worst);
}

/* Get quantity at price. Returns true if found. */
static bool om_ladder_get_qty(const OmMarketLevel *levels, uint32_t count,
                               uint64_t price, bool is_bid, uint64_t *out_qty) {
    bool found = false;
    uint32_t pos = om_ladder_find_pos(levels, count, price, is_bid, &found);
    if (found) {
        *out_qty = levels[pos].qty;
        return true;
    }
    return false;
}

/* ============================================================================
 * Top-N Promotion Logic
 *
 * When a price level is removed from the top-N ladder, we need to scan the
 * order map to find the next-best price that should be promoted into top-N.
 * ============================================================================ */

/* Scan public order map to find promotion candidate and aggregate its quantity.
 * Returns the aggregated quantity at the promotion price, or 0 if no candidate. */
static uint64_t om_market_public_find_promotion(const OmMarketPublicWorker *worker,
                                                 uint16_t product_id,
                                                 bool is_bid,
                                                 const OmMarketLevel *levels,
                                                 uint32_t count,
                                                 uint32_t capacity,
                                                 uint64_t *out_price) {
    if (!worker || !worker->orders || count >= capacity) {
        return 0;  /* No room for promotion */
    }

    uint64_t best_price = 0;
    uint64_t total_qty = 0;
    bool found_candidate = false;

    /* Scan all orders to find the best price not in the current ladder */
    for (khiter_t it = kh_begin(worker->orders); it != kh_end(worker->orders); ++it) {
        if (!kh_exist(worker->orders, it)) {
            continue;
        }
        const OmMarketOrderState *state = &kh_val(worker->orders, it);
        if (state->product_id != product_id ||
            state->side != (is_bid ? OM_SIDE_BID : OM_SIDE_ASK) ||
            !state->active || state->remaining == 0) {
            continue;
        }

        /* Skip if this price is already in the ladder */
        if (om_ladder_contains_price(levels, count, state->price, is_bid)) {
            continue;
        }

        /* Check if this price qualifies (better than worst or have room) */
        if (!om_ladder_price_qualifies(levels, count, capacity, state->price, is_bid)) {
            continue;
        }

        if (!found_candidate) {
            best_price = state->price;
            total_qty = state->remaining;
            found_candidate = true;
        } else if ((is_bid && state->price > best_price) ||
                   (!is_bid && state->price < best_price)) {
            /* Found a better price, reset aggregation */
            best_price = state->price;
            total_qty = state->remaining;
        } else if (state->price == best_price) {
            /* Same price, aggregate quantity */
            total_qty += state->remaining;
        }
    }

    if (found_candidate && out_price) {
        *out_price = best_price;
    }
    return total_qty;
}

/* Scan private worker order map for a specific org to find promotion candidate. */
static uint64_t om_market_worker_find_promotion(const OmMarketWorker *worker,
                                                 uint32_t org_index,
                                                 uint16_t product_id,
                                                 bool is_bid,
                                                 const OmMarketLevel *levels,
                                                 uint32_t count,
                                                 uint64_t *out_price) {
    if (!worker || !worker->orders || org_index >= worker->org_count ||
        count >= worker->top_levels) {
        return 0;  /* No room for promotion */
    }

    khash_t(om_market_order_map) *order_map = worker->orders[org_index];
    if (!order_map) {
        return 0;
    }

    uint64_t best_price = 0;
    uint64_t total_qty = 0;
    bool found_candidate = false;

    /* Scan all orders for this org to find the best price not in the ladder */
    for (khiter_t it = kh_begin(order_map); it != kh_end(order_map); ++it) {
        if (!kh_exist(order_map, it)) {
            continue;
        }
        const OmMarketOrderState *state = &kh_val(order_map, it);
        if (state->product_id != product_id ||
            state->side != (is_bid ? OM_SIDE_BID : OM_SIDE_ASK) ||
            !state->active || state->remaining == 0) {
            continue;
        }

        /* Skip if this price is already in the ladder */
        if (om_ladder_contains_price(levels, count, state->price, is_bid)) {
            continue;
        }

        /* Check if this price qualifies (better than worst or have room) */
        if (!om_ladder_price_qualifies(levels, count, worker->top_levels,
                                        state->price, is_bid)) {
            continue;
        }

        if (!found_candidate) {
            best_price = state->price;
            total_qty = state->remaining;
            found_candidate = true;
        } else if ((is_bid && state->price > best_price) ||
                   (!is_bid && state->price < best_price)) {
            /* Found a better price, reset aggregation */
            best_price = state->price;
            total_qty = state->remaining;
        } else if (state->price == best_price) {
            /* Same price, aggregate quantity */
            total_qty += state->remaining;
        }
    }

    if (found_candidate && out_price) {
        *out_price = best_price;
    }
    return total_qty;
}

/* ============================================================================
 * Private Worker Implementation
 * ============================================================================ */

static int om_market_worker_init(OmMarketWorker *worker,
                                 uint32_t worker_id,
                                 uint16_t max_products,
                                 const OmMarketSubscription *subs,
                                 uint32_t sub_count,
                                 size_t expected_orders,
                                 uint32_t top_levels,
                                 OmMarketDealableFn dealable,
                                 void *dealable_ctx) {
    memset(worker, 0, sizeof(*worker));
    worker->worker_id = worker_id;
    worker->max_products = max_products;
    worker->subscription_count = sub_count;
    worker->dealable = dealable;
    worker->dealable_ctx = dealable_ctx;
    worker->top_levels = top_levels;

    worker->product_offsets = calloc((size_t)max_products + 1U, sizeof(*worker->product_offsets));
    if (!worker->product_offsets) {
        return OM_ERR_PRODUCT_OFFSET;
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
        return OM_ERR_PRODUCT_ORGS;
    }

    worker->product_has_subs = calloc((size_t)max_products, sizeof(*worker->product_has_subs));
    if (!worker->product_has_subs) {
        om_market_worker_destroy(worker);
        return OM_ERR_PRODUCT_SUBS;
    }

    worker->org_ids = calloc(sub_count, sizeof(*worker->org_ids));
    if (!worker->org_ids) {
        om_market_worker_destroy(worker);
        return OM_ERR_ORG_IDS_ALLOC;
    }
    worker->org_index_map = calloc((size_t)UINT16_MAX + 1U, sizeof(*worker->org_index_map));
    if (!worker->org_index_map) {
        om_market_worker_destroy(worker);
        return OM_ERR_ORG_INDEX_ALLOC;
    }
    for (uint32_t i = 0; i <= UINT16_MAX; i++) {
        worker->org_index_map[i] = UINT32_MAX;
    }

    uint32_t *cursor = calloc((size_t)max_products, sizeof(*cursor));
    if (!cursor) {
        om_market_worker_destroy(worker);
        return OM_ERR_ALLOC_FAILED;
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

    /* Allocate ladder metadata array */
    worker->ladders = calloc(sub_count, sizeof(*worker->ladders));
    if (!worker->ladders) {
        om_market_worker_destroy(worker);
        return OM_ERR_LADDER_ALLOC;
    }

    /* Allocate single contiguous block for all levels (cache-line aligned) */
    /* Layout: [ladder0_bid][ladder0_ask][ladder1_bid][ladder1_ask]... */
    size_t levels_per_ladder = (size_t)top_levels * 2;  /* bid + ask */
    size_t total_levels = (size_t)sub_count * levels_per_ladder;
    if (total_levels > 0) {
        worker->levels_block = om_aligned_calloc(total_levels, sizeof(OmMarketLevel));
        if (!worker->levels_block) {
            om_market_worker_destroy(worker);
            return OM_ERR_LADDER_ALLOC;
        }

        /* Assign pointers into the contiguous block */
        for (uint32_t i = 0; i < sub_count; i++) {
            size_t base = i * levels_per_ladder;
            worker->ladders[i].bid_levels = worker->levels_block + base;
            worker->ladders[i].ask_levels = worker->levels_block + base + top_levels;
            worker->ladders[i].bid_count = 0;
            worker->ladders[i].ask_count = 0;
        }
    }

    /* Cache-line aligned dirty flags to prevent false sharing between workers */
    worker->ladder_dirty = om_aligned_calloc(sub_count, sizeof(*worker->ladder_dirty));
    if (!worker->ladder_dirty) {
        om_market_worker_destroy(worker);
        return OM_ERR_LADDER_DIRTY;
    }
    worker->ladder_deltas = calloc(sub_count * 2U, sizeof(*worker->ladder_deltas));
    if (!worker->ladder_deltas) {
        om_market_worker_destroy(worker);
        return OM_ERR_LADDER_DELTA;
    }

    worker->pair_to_ladder = kh_init(om_market_pair_map);
    if (!worker->pair_to_ladder) {
        om_market_worker_destroy(worker);
        return OM_ERR_HASH_INIT;
    }
    for (uint32_t i = 0; i < sub_count; i++) {
        uint32_t key = om_market_pair_key(subs[i].org_id, subs[i].product_id);
        int ret = 0;
        khiter_t it = kh_put(om_market_pair_map, worker->pair_to_ladder, key, &ret);
        if (ret < 0) {
            om_market_worker_destroy(worker);
            return OM_ERR_HASH_PUT;
        }
        kh_val(worker->pair_to_ladder, it) = i;
    }

    worker->orders = calloc(worker->org_count, sizeof(*worker->orders));
    if (!worker->orders) {
        om_market_worker_destroy(worker);
        return OM_ERR_ORDERS_ALLOC;
    }
    for (uint32_t i = 0; i < worker->org_count; i++) {
        worker->orders[i] = kh_init(om_market_order_map);
        if (!worker->orders[i]) {
            om_market_worker_destroy(worker);
            return OM_ERR_HASH_INIT;
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
        return OM_ERR_INDEX_ALLOC;
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

/* ============================================================================
 * Public Worker Implementation
 * ============================================================================ */

static int om_market_public_worker_init(OmMarketPublicWorker *worker,
                                        uint16_t max_products,
                                        uint32_t top_levels,
                                        size_t expected_orders) {
    memset(worker, 0, sizeof(*worker));
    worker->max_products = max_products;
    worker->top_levels = top_levels;

    worker->product_has_subs = calloc((size_t)max_products, sizeof(*worker->product_has_subs));
    if (!worker->product_has_subs) {
        return OM_ERR_PRODUCT_SUBS;
    }
    worker->ladders = calloc((size_t)max_products, sizeof(*worker->ladders));
    if (!worker->ladders) {
        om_market_public_worker_destroy(worker);
        return OM_ERR_LADDER_ALLOC;
    }

    /* Allocate single contiguous block for all levels (cache-line aligned) */
    size_t levels_per_product = (size_t)top_levels * 2;  /* bid + ask */
    size_t total_levels = (size_t)max_products * levels_per_product;
    if (total_levels > 0) {
        worker->levels_block = om_aligned_calloc(total_levels, sizeof(OmMarketLevel));
        if (!worker->levels_block) {
            om_market_public_worker_destroy(worker);
            return OM_ERR_LADDER_ALLOC;
        }

        /* Assign pointers into the contiguous block */
        for (uint32_t i = 0; i < max_products; i++) {
            size_t base = i * levels_per_product;
            worker->ladders[i].bid_levels = worker->levels_block + base;
            worker->ladders[i].ask_levels = worker->levels_block + base + top_levels;
            worker->ladders[i].bid_count = 0;
            worker->ladders[i].ask_count = 0;
        }
    }

    /* Cache-line aligned dirty flags to prevent false sharing between workers */
    worker->dirty = om_aligned_calloc((size_t)max_products, sizeof(*worker->dirty));
    if (!worker->dirty) {
        om_market_public_worker_destroy(worker);
        return OM_ERR_LADDER_DIRTY;
    }
    worker->deltas = calloc((size_t)max_products * 2U, sizeof(*worker->deltas));
    if (!worker->deltas) {
        om_market_public_worker_destroy(worker);
        return OM_ERR_LADDER_DELTA;
    }
    worker->orders = kh_init(om_market_order_map);
    if (!worker->orders) {
        om_market_public_worker_destroy(worker);
        return OM_ERR_HASH_INIT;
    }
    if (expected_orders > 0) {
        kh_resize(om_market_order_map, worker->orders, expected_orders);
    }
    return 0;
}

/* ============================================================================
 * Destroy Functions
 * ============================================================================ */

static void om_market_public_worker_destroy(OmMarketPublicWorker *worker) {
    if (!worker) {
        return;
    }
    /* levels_block is a single allocation, ladders just point into it */
    free(worker->levels_block);
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
    /* levels_block is a single allocation, ladders just point into it */
    free(worker->levels_block);
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

/* ============================================================================
 * Market Init/Destroy
 * ============================================================================ */

int om_market_init(OmMarket *market, const OmMarketConfig *config) {
    if (!market || !config || !config->org_to_worker || !config->subs || config->sub_count == 0) {
        return OM_ERR_NULL_PARAM;
    }
    if (config->worker_count == 0 || config->max_products == 0) {
        return OM_ERR_INVALID_PARAM;
    }
    if (!config->dealable) {
        return OM_ERR_NO_DEALABLE_CB;
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
        return OM_ERR_ALLOC_FAILED;
    }
    market->public_workers = calloc(config->public_worker_count, sizeof(*market->public_workers));
    if (!market->public_workers) {
        free(market->workers);
        market->workers = NULL;
        return OM_ERR_ALLOC_FAILED;
    }

    uint32_t *counts = calloc(config->worker_count, sizeof(*counts));
    if (!counts) {
        free(market->public_workers);
        free(market->workers);
        market->public_workers = NULL;
        market->workers = NULL;
        return OM_ERR_ALLOC_FAILED;
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
            return OM_ERR_WORKER_ID_RANGE;
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
        return OM_ERR_ALLOC_FAILED;
    }
    uint32_t *offsets = calloc(config->worker_count, sizeof(*offsets));
    if (!offsets) {
        free(buckets);
        free(counts);
        free(market->public_workers);
        free(market->workers);
        market->public_workers = NULL;
        market->workers = NULL;
        return OM_ERR_ALLOC_FAILED;
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
                                        config->top_levels,
                                        config->dealable,
                                        config->dealable_ctx);
        if (ret != 0) {
            free(buckets);
            free(offsets);
            free(counts);
            om_market_destroy(market);
            return ret;
        }
        total += count;
    }

    if (!config->product_to_public_worker || config->public_worker_count == 0) {
        free(buckets);
        free(offsets);
        free(counts);
        om_market_destroy(market);
        return OM_ERR_NO_PUBLIC_MAP;
    }
    for (uint32_t w = 0; w < config->public_worker_count; w++) {
        int ret = om_market_public_worker_init(&market->public_workers[w], config->max_products,
                                               config->top_levels,
                                               config->expected_orders_per_worker);
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
        return OM_ERR_PUBLIC_ALLOC;
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
            return OM_ERR_WORKER_ID_RANGE;
        }
        market->public_workers[worker_id].product_has_subs[product_id] = 1U;
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

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

static int om_market_worker_find_ladder(const OmMarketWorker *worker,
                                        uint16_t org_id,
                                        uint16_t product_id,
                                        uint32_t *out_index) {
    if (!worker || !out_index) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t org_index = worker->org_index_map[org_id];
    if (org_index == UINT32_MAX) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    size_t idx = (size_t)org_index * worker->ladder_index_stride + product_id;
    uint32_t ladder_idx = worker->ladder_index[idx];
    if (ladder_idx == UINT32_MAX) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    *out_index = ladder_idx;
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

static int om_market_worker_record_order(OmMarketWorker *worker,
                                         uint32_t org_index,
                                         const OmWalInsert *rec,
                                         OmMarketOrderState *state) {
    if (!worker || !rec || !state) {
        return OM_ERR_NULL_PARAM;
    }
    int ret = 0;
    khiter_t it = kh_put(om_market_order_map, worker->orders[org_index], rec->order_id, &ret);
    if (ret < 0) {
        return OM_ERR_HASH_PUT;
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

/* ============================================================================
 * Process Functions
 * ============================================================================ */

int om_market_worker_process(OmMarketWorker *worker, OmWalType type, const void *data) {
    if (!worker || !data) {
        return OM_ERR_NULL_PARAM;
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
                    return OM_ERR_RECORD_FAILED;
                }
                khiter_t update_it = kh_get(om_market_order_map, worker->orders[org_index], rec->order_id);
                if (update_it != kh_end(worker->orders[org_index])) {
                    kh_val(worker->orders[org_index], update_it).remaining = qty;
                }
                OmMarketLadder *ladder = &worker->ladders[ladder_idx];
                bool is_bid = OM_IS_BID(rec->flags);
                OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
                uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
                om_ladder_add(levels, count, worker->top_levels, rec->price, qty, is_bid);
                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, is_bid);
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
                OmMarketLadder *ladder = &worker->ladders[ladder_idx];
                bool is_bid = state->side == OM_SIDE_BID;
                OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
                uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
                uint16_t product_id = state->product_id;
                uint64_t removed_price = 0;
                int level_removed = om_ladder_sub(levels, count, state->price, state->remaining,
                                                   is_bid, &removed_price);
                uint64_t removed = state->remaining;
                state->remaining = 0;
                state->active = false;
                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, is_bid);
                om_market_delta_add(delta_map, removed_price ? removed_price : state->price,
                                    -(int64_t)removed);

                /* Promote next-best price if a level was removed */
                if (level_removed > 0 && *count < worker->top_levels) {
                    uint64_t promo_price = 0;
                    uint64_t promo_qty = om_market_worker_find_promotion(
                        worker, i, product_id, is_bid, levels, *count, &promo_price);
                    if (promo_qty > 0) {
                        om_ladder_add(levels, count, worker->top_levels, promo_price,
                                      promo_qty, is_bid);
                        om_market_delta_add(delta_map, promo_price, (int64_t)promo_qty);
                    }
                }
                om_market_ladder_mark_dirty(worker, ladder_idx);
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
                OmMarketLadder *ladder = &worker->ladders[ladder_idx];
                bool is_bid = state->side == OM_SIDE_BID;
                OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
                uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
                uint64_t added = state->remaining;
                state->active = true;
                om_ladder_add(levels, count, worker->top_levels, state->price, added, is_bid);
                khash_t(om_market_delta_map) *delta_map =
                    om_market_delta_for_ladder(worker, ladder_idx, is_bid);
                om_market_delta_add(delta_map, state->price, (int64_t)added);
                om_market_ladder_mark_dirty(worker, ladder_idx);
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
                        OmMarketLadder *ladder = &worker->ladders[ladder_idx];
                        bool is_bid = maker->side == OM_SIDE_BID;
                        OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
                        uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
                        uint16_t product_id = maker->product_id;
                        uint64_t match_vol = rec->volume > maker->remaining
                                                 ? maker->remaining : rec->volume;
                        uint64_t removed_price = 0;
                        int level_removed = om_ladder_sub(levels, count, maker->price, match_vol,
                                                           is_bid, &removed_price);
                        maker->remaining -= match_vol;
                        khash_t(om_market_delta_map) *delta_map =
                            om_market_delta_for_ladder(worker, ladder_idx, is_bid);
                        om_market_delta_add(delta_map, maker->price, -(int64_t)match_vol);

                        /* Promote next-best price if a level was removed */
                        if (level_removed > 0 && *count < worker->top_levels) {
                            uint64_t promo_price = 0;
                            uint64_t promo_qty = om_market_worker_find_promotion(
                                worker, i, product_id, is_bid, levels, *count, &promo_price);
                            if (promo_qty > 0) {
                                om_ladder_add(levels, count, worker->top_levels, promo_price,
                                              promo_qty, is_bid);
                                om_market_delta_add(delta_map, promo_price, (int64_t)promo_qty);
                            }
                        }
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
        return OM_ERR_NULL_PARAM;
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
                return OM_ERR_HASH_PUT;
            }
            OmMarketOrderState pub_state = {
                .product_id = rec->product_id,
                .side = OM_GET_SIDE(rec->flags),
                .active = true,
                .price = rec->price,
                .remaining = rec->vol_remain
            };
            kh_val(worker->orders, pub_it) = pub_state;
            OmMarketLadder *ladder = &worker->ladders[rec->product_id];
            bool is_bid = OM_IS_BID(rec->flags);
            OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
            uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
            om_ladder_add(levels, count, worker->top_levels, rec->price, rec->vol_remain, is_bid);
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, rec->product_id, is_bid);
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
            uint16_t product_id = pub_state->product_id;
            OmMarketLadder *ladder = &worker->ladders[product_id];
            bool is_bid = pub_state->side == OM_SIDE_BID;
            OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
            uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
            uint64_t removed_price = 0;
            int level_removed = om_ladder_sub(levels, count, pub_state->price, pub_state->remaining,
                                               is_bid, &removed_price);
            uint64_t removed = pub_state->remaining;
            pub_state->remaining = 0;
            pub_state->active = false;
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, product_id, is_bid);
            om_market_delta_add(delta_map, removed_price ? removed_price : pub_state->price,
                                -(int64_t)removed);

            /* Promote next-best price if a level was removed */
            if (level_removed > 0 && *count < worker->top_levels) {
                uint64_t promo_price = 0;
                uint64_t promo_qty = om_market_public_find_promotion(
                    worker, product_id, is_bid, levels, *count, worker->top_levels, &promo_price);
                if (promo_qty > 0) {
                    om_ladder_add(levels, count, worker->top_levels, promo_price,
                                  promo_qty, is_bid);
                    om_market_delta_add(delta_map, promo_price, (int64_t)promo_qty);
                }
            }
            om_market_public_mark_dirty(worker, product_id);
            return 0;
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
            OmMarketLadder *ladder = &worker->ladders[pub_state->product_id];
            bool is_bid = pub_state->side == OM_SIDE_BID;
            OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
            uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
            uint64_t added = pub_state->remaining;
            om_ladder_add(levels, count, worker->top_levels, pub_state->price, added, is_bid);
            pub_state->active = true;
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, pub_state->product_id, is_bid);
            om_market_delta_add(delta_map, pub_state->price, (int64_t)added);
            om_market_public_mark_dirty(worker, pub_state->product_id);
            return 0;
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
            uint16_t product_id = pub_state->product_id;
            OmMarketLadder *ladder = &worker->ladders[product_id];
            bool is_bid = pub_state->side == OM_SIDE_BID;
            OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
            uint32_t *count = is_bid ? &ladder->bid_count : &ladder->ask_count;
            uint64_t match_vol = rec->volume > pub_state->remaining
                                     ? pub_state->remaining
                                     : rec->volume;
            uint64_t removed_price = 0;
            int level_removed = om_ladder_sub(levels, count, pub_state->price, match_vol,
                                               is_bid, &removed_price);
            pub_state->remaining -= match_vol;
            khash_t(om_market_delta_map) *delta_map =
                om_market_delta_for_public(worker, product_id, is_bid);
            om_market_delta_add(delta_map, pub_state->price, -(int64_t)match_vol);

            /* Promote next-best price if a level was removed */
            if (level_removed > 0 && *count < worker->top_levels) {
                uint64_t promo_price = 0;
                uint64_t promo_qty = om_market_public_find_promotion(
                    worker, product_id, is_bid, levels, *count, worker->top_levels, &promo_price);
                if (promo_qty > 0) {
                    om_ladder_add(levels, count, worker->top_levels, promo_price,
                                  promo_qty, is_bid);
                    om_market_delta_add(delta_map, promo_price, (int64_t)promo_qty);
                }
            }
            om_market_public_mark_dirty(worker, product_id);
            return 0;
        }
        default:
            return 0;
    }
}

/* ============================================================================
 * Query Functions
 * ============================================================================ */

int om_market_worker_get_qty(const OmMarketWorker *worker,
                             uint16_t org_id,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty) {
    if (!worker || !out_qty) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    const OmMarketLadder *ladder = &worker->ladders[ladder_idx];
    bool is_bid = side == OM_SIDE_BID;
    const OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
    uint32_t count = is_bid ? ladder->bid_count : ladder->ask_count;

    if (om_ladder_get_qty(levels, count, price, is_bid, out_qty)) {
        return 0;
    }
    return OM_ERR_NOT_FOUND;
}

int om_market_public_get_qty(const OmMarketPublicWorker *worker,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty) {
    if (!worker || !out_qty || !worker->ladders) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    const OmMarketLadder *ladder = &worker->ladders[product_id];
    bool is_bid = side == OM_SIDE_BID;
    const OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
    uint32_t count = is_bid ? ladder->bid_count : ladder->ask_count;

    if (om_ladder_get_qty(levels, count, price, is_bid, out_qty)) {
        return 0;
    }
    return OM_ERR_NOT_FOUND;
}

int om_market_worker_is_subscribed(const OmMarketWorker *worker,
                                  uint16_t org_id,
                                  uint16_t product_id) {
    if (!worker) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return 0;
    }
    return 1;
}

/* ============================================================================
 * Delta Functions
 * ============================================================================ */

int om_market_worker_delta_count(const OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id,
                                 uint16_t side) {
    if (!worker || !worker->ladder_deltas) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
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
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
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
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
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
    if (!worker || !worker->deltas) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
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
    if (!worker || !out || !worker->deltas) {
        return OM_ERR_NULL_PARAM;
    }
    if (max == 0) {
        return OM_ERR_INVALID_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
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
    if (!worker || !worker->deltas) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    uint32_t idx = (uint32_t)product_id * 2U + (side == OM_SIDE_BID ? 0U : 1U);
    khash_t(om_market_delta_map) *map = worker->deltas[idx];
    if (!map) {
        return 0;
    }
    kh_clear(om_market_delta_map, map);
    return 0;
}

/* ============================================================================
 * Copy Full Ladder (Cache-Optimized Sequential Access)
 * ============================================================================ */

int om_market_worker_copy_full(const OmMarketWorker *worker,
                               uint16_t org_id,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max) {
    if (!worker || !out || max == 0) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    const OmMarketLadder *ladder = &worker->ladders[ladder_idx];
    bool is_bid = side == OM_SIDE_BID;
    const OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
    uint32_t count = is_bid ? ladder->bid_count : ladder->ask_count;

    /* Simple sequential copy - cache optimal */
    size_t copy_count = count < max ? count : max;
    for (size_t i = 0; i < copy_count; i++) {
        out[i].price = levels[i].price;
        out[i].delta = (int64_t)levels[i].qty;
    }
    return (int)copy_count;
}

int om_market_public_copy_full(const OmMarketPublicWorker *worker,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max) {
    if (!worker || !out) {
        return OM_ERR_NULL_PARAM;
    }
    if (max == 0) {
        return OM_ERR_INVALID_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    const OmMarketLadder *ladder = &worker->ladders[product_id];
    bool is_bid = side == OM_SIDE_BID;
    const OmMarketLevel *levels = is_bid ? ladder->bid_levels : ladder->ask_levels;
    uint32_t count = is_bid ? ladder->bid_count : ladder->ask_count;

    /* Simple sequential copy - cache optimal */
    size_t copy_count = count < max ? count : max;
    for (size_t i = 0; i < copy_count; i++) {
        out[i].price = levels[i].price;
        out[i].delta = (int64_t)levels[i].qty;
    }
    return (int)copy_count;
}

/* ============================================================================
 * Dirty Flag Functions
 * ============================================================================ */

int om_market_worker_is_dirty(const OmMarketWorker *worker,
                              uint16_t org_id,
                              uint16_t product_id) {
    if (!worker || !worker->ladder_dirty) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    return worker->ladder_dirty[ladder_idx] ? 1 : 0;
}

int om_market_worker_clear_dirty(OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id) {
    if (!worker || !worker->ladder_dirty) {
        return OM_ERR_NULL_PARAM;
    }
    uint32_t ladder_idx = 0;
    if (om_market_worker_find_ladder(worker, org_id, product_id, &ladder_idx) != 0) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    worker->ladder_dirty[ladder_idx] = 0;
    return 0;
}

int om_market_public_is_dirty(const OmMarketPublicWorker *worker, uint16_t product_id) {
    if (!worker || !worker->dirty) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    return worker->dirty[product_id] ? 1 : 0;
}

int om_market_public_clear_dirty(OmMarketPublicWorker *worker, uint16_t product_id) {
    if (!worker || !worker->dirty) {
        return OM_ERR_NULL_PARAM;
    }
    if (product_id >= worker->max_products) {
        return OM_ERR_OUT_OF_RANGE;
    }
    if (worker->product_has_subs && !worker->product_has_subs[product_id]) {
        return OM_ERR_NOT_SUBSCRIBED;
    }
    worker->dirty[product_id] = 0;
    return 0;
}
