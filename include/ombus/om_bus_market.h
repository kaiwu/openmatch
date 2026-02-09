#ifndef OM_BUS_MARKET_H
#define OM_BUS_MARKET_H

/**
 * @file om_bus_market.h
 * @brief Header-only helpers: poll bus endpoint → feed market worker
 *
 * Provides convenience functions that poll one record from an OmBusEndpoint
 * and feed it directly to om_market_worker_process() or
 * om_market_public_process().
 */

#include "ombus/om_bus.h"
#include "openmarket/om_market.h"
#include "openmatch/om_wal.h"

/**
 * Poll one record from bus endpoint and process it with a private worker.
 * @param ep Bus endpoint (consumer side)
 * @param w  Private market worker
 * @return 1 if a record was processed, 0 if empty, negative on error
 */
static inline int om_bus_poll_worker(OmBusEndpoint *ep, OmMarketWorker *w) {
    OmBusRecord rec;
    int rc = om_bus_endpoint_poll(ep, &rec);
    if (rc <= 0) return rc;
    int prc = om_market_worker_process(w, (OmWalType)rec.wal_type, rec.payload);
    return prc < 0 ? prc : 1;
}

/**
 * Poll one record from bus endpoint and process it with a public worker.
 * @param ep Bus endpoint (consumer side)
 * @param w  Public market worker
 * @return 1 if a record was processed, 0 if empty, negative on error
 */
static inline int om_bus_poll_public(OmBusEndpoint *ep, OmMarketPublicWorker *w) {
    OmBusRecord rec;
    int rc = om_bus_endpoint_poll(ep, &rec);
    if (rc <= 0) return rc;
    int prc = om_market_public_process(w, (OmWalType)rec.wal_type, rec.payload);
    return prc < 0 ? prc : 1;
}

#endif /* OM_BUS_MARKET_H */
