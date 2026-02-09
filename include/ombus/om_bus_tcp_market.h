#ifndef OM_BUS_TCP_MARKET_H
#define OM_BUS_TCP_MARKET_H

/**
 * @file om_bus_tcp_market.h
 * @brief Header-only helpers: poll TCP client -> feed market worker
 *
 * Same pattern as om_bus_market.h but for TCP transport.
 */

#include "ombus/om_bus_tcp.h"
#include "openmarket/om_market.h"
#include "openmatch/om_wal.h"

/**
 * Poll one record from TCP client and process it with a private worker.
 * @param client TCP client handle
 * @param w      Private market worker
 * @return 1 if a record was processed, 0 if empty, negative on error
 */
static inline int om_bus_tcp_poll_worker(OmBusTcpClient *client, OmMarketWorker *w) {
    OmBusRecord rec;
    int rc = om_bus_tcp_client_poll(client, &rec);
    if (rc <= 0) return rc;
    int prc = om_market_worker_process(w, (OmWalType)rec.wal_type, rec.payload);
    return prc < 0 ? prc : 1;
}

/**
 * Poll one record from TCP client and process it with a public worker.
 * @param client TCP client handle
 * @param w      Public market worker
 * @return 1 if a record was processed, 0 if empty, negative on error
 */
static inline int om_bus_tcp_poll_public(OmBusTcpClient *client, OmMarketPublicWorker *w) {
    OmBusRecord rec;
    int rc = om_bus_tcp_client_poll(client, &rec);
    if (rc <= 0) return rc;
    int prc = om_market_public_process(w, (OmWalType)rec.wal_type, rec.payload);
    return prc < 0 ? prc : 1;
}

#endif /* OM_BUS_TCP_MARKET_H */
