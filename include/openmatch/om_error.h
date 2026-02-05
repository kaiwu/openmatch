#ifndef OM_ERROR_H
#define OM_ERROR_H

/**
 * @file om_error.h
 * @brief Error codes for OpenMatch and OpenMarket modules
 *
 * All error codes are negative integers. Zero indicates success.
 * Use om_error_string() to get a human-readable description.
 *
 * Error code ranges:
 *   -1 to -99:   General/common errors
 *   -100 to -199: Slab allocator errors
 *   -200 to -299: WAL errors
 *   -300 to -399: Orderbook errors
 *   -400 to -499: Engine errors
 *   -500 to -599: Market/Worker errors
 *   -600 to -699: Ring buffer errors
 */

/**
 * Error codes enum
 */
typedef enum OmError {
    /* Success */
    OM_OK = 0,

    /* General errors (-1 to -99) */
    OM_ERR_NULL_PARAM       = -1,   /**< NULL pointer passed as required parameter */
    OM_ERR_INVALID_PARAM    = -2,   /**< Invalid parameter value */
    OM_ERR_ALLOC_FAILED     = -3,   /**< Memory allocation failed */
    OM_ERR_NOT_FOUND        = -4,   /**< Requested item not found */
    OM_ERR_ALREADY_EXISTS   = -5,   /**< Item already exists */
    OM_ERR_OUT_OF_RANGE     = -6,   /**< Index or value out of valid range */
    OM_ERR_NOT_SUBSCRIBED   = -7,   /**< Not subscribed to resource */
    OM_ERR_INVALID_STATE    = -8,   /**< Operation invalid in current state */

    /* Slab allocator errors (-100 to -199) */
    OM_ERR_SLAB_INIT        = -100, /**< Slab initialization failed */
    OM_ERR_SLAB_FULL        = -101, /**< Slab has no free slots */
    OM_ERR_SLAB_INVALID_IDX = -102, /**< Invalid slot index */
    OM_ERR_SLAB_AUX_ALLOC   = -103, /**< Aux slab allocation failed */

    /* WAL errors (-200 to -299) */
    OM_ERR_WAL_INIT         = -200, /**< WAL initialization failed */
    OM_ERR_WAL_OPEN         = -201, /**< WAL file open failed */
    OM_ERR_WAL_WRITE        = -202, /**< WAL write failed */
    OM_ERR_WAL_READ         = -203, /**< WAL read failed */
    OM_ERR_WAL_FLUSH        = -204, /**< WAL flush failed */
    OM_ERR_WAL_FSYNC        = -205, /**< WAL fsync failed */
    OM_ERR_WAL_CRC_MISMATCH = -206, /**< WAL CRC32 checksum mismatch */
    OM_ERR_WAL_INVALID_TYPE = -207, /**< Invalid WAL record type */
    OM_ERR_WAL_TRUNCATED    = -208, /**< WAL record truncated */
    OM_ERR_WAL_BUFFER_ALLOC = -209, /**< WAL buffer allocation failed */

    /* Orderbook errors (-300 to -399) */
    OM_ERR_ORDERBOOK_INIT   = -300, /**< Orderbook initialization failed */
    OM_ERR_ORDERBOOK_FULL   = -301, /**< Orderbook is full */
    OM_ERR_ORDER_NOT_FOUND  = -302, /**< Order not found in book */
    OM_ERR_PRICE_NOT_FOUND  = -303, /**< Price level not found */
    OM_ERR_PRODUCT_ALLOC    = -304, /**< Product array allocation failed */
    OM_ERR_ORG_ALLOC        = -305, /**< Org heads allocation failed */
    OM_ERR_RECOVERY_FAILED  = -306, /**< WAL recovery failed */

    /* Engine errors (-400 to -499) */
    OM_ERR_ENGINE_INIT      = -400, /**< Engine initialization failed */
    OM_ERR_ENGINE_WAL_INIT  = -401, /**< Engine WAL initialization failed */
    OM_ERR_ENGINE_OB_INIT   = -402, /**< Engine orderbook initialization failed */
    OM_ERR_MATCH_FAILED     = -403, /**< Matching operation failed */
    OM_ERR_RECORD_FAILED    = -404, /**< Order recording failed */

    /* Market/Worker errors (-500 to -599) */
    OM_ERR_MARKET_INIT      = -500, /**< Market initialization failed */
    OM_ERR_WORKER_INIT      = -501, /**< Worker initialization failed */
    OM_ERR_NO_DEALABLE_CB   = -502, /**< No dealable callback provided */
    OM_ERR_WORKER_ID_RANGE  = -503, /**< Worker ID out of range */
    OM_ERR_HASH_INIT        = -504, /**< Hash table initialization failed */
    OM_ERR_HASH_PUT         = -505, /**< Hash table put operation failed */
    OM_ERR_LADDER_ALLOC     = -506, /**< Ladder allocation failed */
    OM_ERR_LADDER_DIRTY     = -507, /**< Ladder dirty array allocation failed */
    OM_ERR_LADDER_DELTA     = -508, /**< Ladder delta array allocation failed */
    OM_ERR_ORDERS_ALLOC     = -509, /**< Orders array allocation failed */
    OM_ERR_INDEX_ALLOC      = -510, /**< Index array allocation failed */
    OM_ERR_PRODUCT_OFFSET   = -511, /**< Product offsets allocation failed */
    OM_ERR_PRODUCT_ORGS     = -512, /**< Product orgs allocation failed */
    OM_ERR_PRODUCT_SUBS     = -513, /**< Product subscriptions allocation failed */
    OM_ERR_ORG_IDS_ALLOC    = -514, /**< Org IDs array allocation failed */
    OM_ERR_ORG_INDEX_ALLOC  = -515, /**< Org index map allocation failed */
    OM_ERR_NO_PUBLIC_MAP    = -516, /**< No product_to_public_worker map provided */
    OM_ERR_PUBLIC_ALLOC     = -517, /**< Public products allocation failed */

    /* Ring buffer errors (-600 to -699) */
    OM_ERR_RING_INIT        = -600, /**< Ring buffer initialization failed */
    OM_ERR_RING_NOT_POW2    = -601, /**< Ring capacity not power of two */
    OM_ERR_RING_SLOTS_ALLOC = -602, /**< Ring slots allocation failed */
    OM_ERR_RING_TAILS_ALLOC = -603, /**< Ring consumer tails allocation failed */
    OM_ERR_RING_MUTEX_INIT  = -604, /**< Ring mutex initialization failed */
    OM_ERR_RING_COND_INIT   = -605, /**< Ring condition var initialization failed */
    OM_ERR_RING_CONSUMER_ID = -606, /**< Invalid consumer index */

    /* Perf config errors (-700 to -799) */
    OM_ERR_PERF_CONFIG      = -700, /**< Performance config validation failed */

    /* Reserved for future use */
    OM_ERR_UNKNOWN          = -999  /**< Unknown error */
} OmError;

/**
 * Get human-readable error string
 *
 * @param err Error code
 * @return Static string describing the error
 */
static inline const char *om_error_string(OmError err) {
    switch (err) {
        case OM_OK:                  return "Success";
        case OM_ERR_NULL_PARAM:      return "NULL parameter";
        case OM_ERR_INVALID_PARAM:   return "Invalid parameter";
        case OM_ERR_ALLOC_FAILED:    return "Memory allocation failed";
        case OM_ERR_NOT_FOUND:       return "Not found";
        case OM_ERR_ALREADY_EXISTS:  return "Already exists";
        case OM_ERR_OUT_OF_RANGE:    return "Out of range";
        case OM_ERR_NOT_SUBSCRIBED:  return "Not subscribed";
        case OM_ERR_INVALID_STATE:   return "Invalid state";
        case OM_ERR_SLAB_INIT:       return "Slab initialization failed";
        case OM_ERR_SLAB_FULL:       return "Slab full";
        case OM_ERR_SLAB_INVALID_IDX: return "Invalid slot index";
        case OM_ERR_SLAB_AUX_ALLOC:  return "Aux slab allocation failed";
        case OM_ERR_WAL_INIT:        return "WAL initialization failed";
        case OM_ERR_WAL_OPEN:        return "WAL file open failed";
        case OM_ERR_WAL_WRITE:       return "WAL write failed";
        case OM_ERR_WAL_READ:        return "WAL read failed";
        case OM_ERR_WAL_FLUSH:       return "WAL flush failed";
        case OM_ERR_WAL_FSYNC:       return "WAL fsync failed";
        case OM_ERR_WAL_CRC_MISMATCH: return "WAL CRC32 mismatch";
        case OM_ERR_WAL_INVALID_TYPE: return "Invalid WAL record type";
        case OM_ERR_WAL_TRUNCATED:   return "WAL record truncated";
        case OM_ERR_WAL_BUFFER_ALLOC: return "WAL buffer allocation failed";
        case OM_ERR_ORDERBOOK_INIT:  return "Orderbook initialization failed";
        case OM_ERR_ORDERBOOK_FULL:  return "Orderbook full";
        case OM_ERR_ORDER_NOT_FOUND: return "Order not found";
        case OM_ERR_PRICE_NOT_FOUND: return "Price level not found";
        case OM_ERR_PRODUCT_ALLOC:   return "Product array allocation failed";
        case OM_ERR_ORG_ALLOC:       return "Org heads allocation failed";
        case OM_ERR_RECOVERY_FAILED: return "WAL recovery failed";
        case OM_ERR_ENGINE_INIT:     return "Engine initialization failed";
        case OM_ERR_ENGINE_WAL_INIT: return "Engine WAL init failed";
        case OM_ERR_ENGINE_OB_INIT:  return "Engine orderbook init failed";
        case OM_ERR_MATCH_FAILED:    return "Matching failed";
        case OM_ERR_RECORD_FAILED:   return "Order recording failed";
        case OM_ERR_MARKET_INIT:     return "Market initialization failed";
        case OM_ERR_WORKER_INIT:     return "Worker initialization failed";
        case OM_ERR_NO_DEALABLE_CB:  return "No dealable callback";
        case OM_ERR_WORKER_ID_RANGE: return "Worker ID out of range";
        case OM_ERR_HASH_INIT:       return "Hash table init failed";
        case OM_ERR_HASH_PUT:        return "Hash table put failed";
        case OM_ERR_LADDER_ALLOC:    return "Ladder allocation failed";
        case OM_ERR_LADDER_DIRTY:    return "Ladder dirty alloc failed";
        case OM_ERR_LADDER_DELTA:    return "Ladder delta alloc failed";
        case OM_ERR_ORDERS_ALLOC:    return "Orders array alloc failed";
        case OM_ERR_INDEX_ALLOC:     return "Index array alloc failed";
        case OM_ERR_PRODUCT_OFFSET:  return "Product offsets alloc failed";
        case OM_ERR_PRODUCT_ORGS:    return "Product orgs alloc failed";
        case OM_ERR_PRODUCT_SUBS:    return "Product subs alloc failed";
        case OM_ERR_ORG_IDS_ALLOC:   return "Org IDs alloc failed";
        case OM_ERR_ORG_INDEX_ALLOC: return "Org index map alloc failed";
        case OM_ERR_NO_PUBLIC_MAP:   return "No public worker map";
        case OM_ERR_PUBLIC_ALLOC:    return "Public products alloc failed";
        case OM_ERR_RING_INIT:       return "Ring buffer init failed";
        case OM_ERR_RING_NOT_POW2:   return "Ring capacity not power of 2";
        case OM_ERR_RING_SLOTS_ALLOC: return "Ring slots alloc failed";
        case OM_ERR_RING_TAILS_ALLOC: return "Ring tails alloc failed";
        case OM_ERR_RING_MUTEX_INIT: return "Ring mutex init failed";
        case OM_ERR_RING_COND_INIT:  return "Ring cond init failed";
        case OM_ERR_RING_CONSUMER_ID: return "Invalid consumer index";
        case OM_ERR_PERF_CONFIG:     return "Perf config validation failed";
        case OM_ERR_UNKNOWN:         return "Unknown error";
        default:                     return "Unrecognized error code";
    }
}

#endif /* OM_ERROR_H */
