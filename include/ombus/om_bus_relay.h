#ifndef OM_BUS_RELAY_H
#define OM_BUS_RELAY_H

/**
 * @file om_bus_relay.h
 * @brief Header-only relay: SHM endpoint -> TCP server broadcast
 *
 * Polls records from a local SHM endpoint and broadcasts them to all
 * connected TCP clients. Runs in a loop until *running is set to false.
 */

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

#include "ombus/om_bus.h"
#include "ombus/om_bus_tcp.h"

typedef struct OmBusRelayConfig {
    OmBusEndpoint     *ep;           /* SHM consumer */
    OmBusTcpServer    *srv;          /* TCP broadcaster */
    volatile bool     *running;      /* shutdown flag (NULL = run forever) */
    uint32_t           poll_us;      /* sleep between empty polls (0 = default 10us) */
} OmBusRelayConfig;

/**
 * Run the relay loop: poll SHM -> broadcast TCP.
 *
 * Returns 0 on clean shutdown (*running = false),
 * negative on SHM poll error (e.g., OM_ERR_BUS_EPOCH_CHANGED).
 * TCP poll_io errors are ignored (clients reconnect).
 */
static inline int om_bus_relay_run(const OmBusRelayConfig *cfg) {
    if (!cfg || !cfg->ep || !cfg->srv) return -1;

    uint32_t poll_us = cfg->poll_us ? cfg->poll_us : 10;
    uint32_t idle_spins = 0;

    while (!cfg->running || *cfg->running) {
        OmBusRecord rec;
        int rc = om_bus_endpoint_poll(cfg->ep, &rec);

        if (rc == 1) {
            om_bus_tcp_server_broadcast(cfg->srv, rec.wal_seq,
                                         rec.wal_type, rec.payload, rec.payload_len);
            om_bus_tcp_server_poll_io(cfg->srv);
            idle_spins = 0;
        } else if (rc == 0) {
            /* Empty — flush any pending TCP data and sleep */
            if (idle_spins == 0) {
                om_bus_tcp_server_poll_io(cfg->srv);
            }
            idle_spins++;
            if (idle_spins > 100) {
                usleep(poll_us);
            }
        } else {
            /* SHM error (gap, epoch, CRC) — propagate to caller */
            return rc;
        }
    }

    /* Final flush */
    om_bus_tcp_server_poll_io(cfg->srv);
    return 0;
}

#endif /* OM_BUS_RELAY_H */
