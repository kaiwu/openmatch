#ifndef OM_MARKET_H
#define OM_MARKET_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <khash.h>
#include "openmatch/om_wal.h"
#include "openmatch/om_slab.h"

/**
 * @file om_market.h
 * @brief OpenMarket public API
 */

#define OM_MARKET_VERSION_MAJOR 1U
#define OM_MARKET_VERSION_MINOR 0U
#define OM_MARKET_VERSION_PATCH 0U

typedef struct OmMarketVersion {
    uint32_t major;
    uint32_t minor;
    uint32_t patch;
} OmMarketVersion;

typedef struct OmMarketSubscription {
    uint16_t org_id;
    uint16_t product_id;
} OmMarketSubscription;

typedef struct OmMarketOrderState {
    uint16_t product_id;
    uint16_t side;
    bool active;
    uint64_t price;
    uint64_t remaining;
} OmMarketOrderState;

KHASH_MAP_INIT_INT64(om_market_price_map, uint64_t)
KHASH_MAP_INIT_INT64(om_market_order_map, OmMarketOrderState)
KHASH_MAP_INIT_INT(om_market_pair_map, uint32_t)
KHASH_MAP_INIT_INT64(om_market_delta_map, int64_t)

typedef struct OmMarketLadder {
    khash_t(om_market_price_map) *bid;
    khash_t(om_market_price_map) *ask;
} OmMarketLadder;

typedef struct OmMarketDelta {
    uint64_t price;
    int64_t delta;
} OmMarketDelta;


typedef uint64_t (*OmMarketDealableFn)(const OmWalInsert *rec, uint16_t viewer_org, void *ctx);

typedef struct OmMarketWorker {
    uint32_t worker_id;
    uint16_t max_products;
    uint32_t subscription_count;
    uint32_t org_count;
    uint32_t *product_offsets;
    uint16_t *product_orgs;
    uint16_t *org_ids;
    uint32_t *org_index_map;
    uint32_t *ladder_index;
    size_t ladder_index_stride;
    uint8_t *product_has_subs;
    uint32_t top_levels;
    OmMarketLadder *ladders;
    uint8_t *ladder_dirty;
    khash_t(om_market_delta_map) **ladder_deltas;
    khash_t(om_market_pair_map) *pair_to_ladder;
    khash_t(om_market_order_map) **orders;
    OmMarketDealableFn dealable;
    void *dealable_ctx;
} OmMarketWorker;

typedef struct OmMarketPublicWorker {
    uint16_t max_products;
    uint8_t *product_has_subs;
    uint32_t top_levels;
    OmMarketLadder *ladders;
    uint8_t *dirty;
    khash_t(om_market_delta_map) **deltas;
    khash_t(om_market_order_map) *orders;
} OmMarketPublicWorker;

typedef struct OmMarketConfig {
    uint16_t max_products;
    uint32_t worker_count;
    uint32_t public_worker_count;
    const uint32_t *org_to_worker;        /**< Array indexed by org_id */
    const uint32_t *product_to_public_worker; /**< Array indexed by product_id */
    const OmMarketSubscription *subs;
    uint32_t sub_count;
    size_t expected_orders_per_worker;
    size_t expected_subscribers_per_product; /**< Avg orgs per product */
    size_t expected_price_levels;
    uint32_t top_levels;                      /**< Top N price levels to aggregate */
    bool enable_full_snapshot;                /**< Enable full snapshot publishing (optional) */
    OmMarketDealableFn dealable;
    void *dealable_ctx;
} OmMarketConfig;

typedef struct OmMarket {
    OmMarketWorker *workers;
    uint32_t worker_count;
    OmMarketPublicWorker *public_workers;
    uint32_t public_worker_count;
    uint16_t max_products;
    uint32_t top_levels;
    bool enable_full_snapshot;
    OmMarketDealableFn dealable;
    void *dealable_ctx;
} OmMarket;


/**
 * Get OpenMarket version as a struct.
 * @return Version struct (major, minor, patch)
 */
OmMarketVersion om_market_version(void);

/**
 * Get OpenMarket version string.
 * @return Null-terminated version string
 */
const char *om_market_version_string(void);

/**
 * Initialize market aggregation workers.
 * @param market Market instance
 * @param config Configuration (subscriptions, workers, sizing)
 * @return 0 on success, negative on error
 */
int om_market_init(OmMarket *market, const OmMarketConfig *config);

/**
 * Destroy market aggregation workers.
 * @param market Market instance
 */
void om_market_destroy(OmMarket *market);

/**
 * Get worker by id.
 * @param market Market instance
 * @param worker_id Worker id
 * @return Worker pointer or NULL
 */
OmMarketWorker *om_market_worker(OmMarket *market, uint32_t worker_id);

/**
 * Process a WAL record with a worker.
 * @param worker Worker instance
 * @param type WAL record type
 * @param data WAL record data
 * @return 0 on success, negative on error
 */
int om_market_worker_process(OmMarketWorker *worker, OmWalType type, const void *data);

int om_market_public_process(OmMarketPublicWorker *worker, OmWalType type, const void *data);

/**
 * Get aggregated quantity for a worker's org/product/price ladder.
 * @param worker Worker instance
 * @param org_id Org id
 * @param product_id Product id
 * @param side OM_SIDE_BID/OM_SIDE_ASK
 * @param price Price level
 * @param out_qty Output quantity
 * @return 0 if found, -1 if not found or invalid
 */
int om_market_worker_get_qty(const OmMarketWorker *worker,
                             uint16_t org_id,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty);

/**
 * Get public aggregated quantity for a product/price ladder.
 * @param worker Worker instance
 * @param product_id Product id
 * @param side OM_SIDE_BID/OM_SIDE_ASK
 * @param price Price level
 * @param out_qty Output quantity
 * @return 0 if found, -1 if not found or invalid
 */
int om_market_public_get_qty(const OmMarketPublicWorker *worker,
                             uint16_t product_id,
                             uint16_t side,
                             uint64_t price,
                             uint64_t *out_qty);

/**
 * Check whether an org subscribes to a product in this worker.
 * @param worker Worker instance
 * @param org_id Org id
 * @param product_id Product id
 * @return 1 if subscribed, 0 if not, negative on error
 */
int om_market_worker_is_subscribed(const OmMarketWorker *worker,
                                  uint16_t org_id,
                                  uint16_t product_id);

/**
 * Get count of private ladder deltas for (org, product, side).
 * @return count on success, 0 if none, negative on error
 */
int om_market_worker_delta_count(const OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id,
                                 uint16_t side);

/**
 * Copy private ladder deltas into caller buffer.
 * @return number of deltas copied, negative on error
 */
int om_market_worker_copy_deltas(const OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id,
                                 uint16_t side,
                                 OmMarketDelta *out,
                                 size_t max);

/**
 * Clear private ladder deltas after publish.
 * @return 0 on success, negative on error
 */
int om_market_worker_clear_deltas(OmMarketWorker *worker,
                                  uint16_t org_id,
                                  uint16_t product_id,
                                  uint16_t side);

/**
 * Get count of public ladder deltas for product/side.
 * @return count on success, 0 if none, negative on error
 */
int om_market_public_delta_count(const OmMarketPublicWorker *worker,
                                 uint16_t product_id,
                                 uint16_t side);

/**
 * Copy public ladder deltas into caller buffer.
 * @return number of deltas copied, negative on error
 */
int om_market_public_copy_deltas(const OmMarketPublicWorker *worker,
                                 uint16_t product_id,
                                 uint16_t side,
                                 OmMarketDelta *out,
                                 size_t max);

/**
 * Clear public ladder deltas after publish.
 * @return 0 on success, negative on error
 */
int om_market_public_clear_deltas(OmMarketPublicWorker *worker,
                                  uint16_t product_id,
                                  uint16_t side);

/**
 * Copy full private ladder (top-N) for (org, product, side).
 * @return number of levels copied, negative on error
 */
int om_market_worker_copy_full(const OmMarketWorker *worker,
                               uint16_t org_id,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max);

/**
 * Copy full public ladder (top-N) for product/side.
 * @return number of levels copied, negative on error
 */
int om_market_public_copy_full(const OmMarketPublicWorker *worker,
                               uint16_t product_id,
                               uint16_t side,
                               OmMarketDelta *out,
                               size_t max);

/**
 * Check whether a ladder changed since last publish.
 * @param worker Worker instance
 * @param org_id Org id
 * @param product_id Product id
 * @return 1 if dirty, 0 if clean, negative on error
 */
int om_market_worker_is_dirty(const OmMarketWorker *worker,
                              uint16_t org_id,
                              uint16_t product_id);

/**
 * Clear dirty flag after publishing.
 * @param worker Worker instance
 * @param org_id Org id
 * @param product_id Product id
 * @return 0 on success, negative on error
 */
int om_market_worker_clear_dirty(OmMarketWorker *worker,
                                 uint16_t org_id,
                                 uint16_t product_id);

/**
 * Check whether a public ladder changed since last publish.
 * @param worker Worker instance
 * @param product_id Product id
 * @return 1 if dirty, 0 if clean, negative on error
 */
int om_market_public_is_dirty(const OmMarketPublicWorker *worker, uint16_t product_id);
int om_market_public_clear_dirty(OmMarketPublicWorker *worker, uint16_t product_id);


#endif
