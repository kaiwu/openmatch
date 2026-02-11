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
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "ombus/om_bus.h"
#include "ombus/om_bus_tcp.h"

typedef struct OmBusRelayConfig {
    OmBusEndpoint     *ep;           /* SHM consumer */
    OmBusTcpServer    *srv;          /* TCP broadcaster */
    volatile bool     *running;      /* shutdown flag (NULL = run forever) */
    uint32_t           poll_us;      /* sleep between empty polls (0 = default 10us) */
    struct OmBusRelayStats *stats;
} OmBusRelayConfig;

typedef struct OmBusRelayStats {
    uint64_t loops;
    uint64_t non_empty_loops;
    uint64_t records_relayed;
    uint64_t idle_loops;
    uint64_t loop_ns_total;
    uint64_t loop_ns_max;
    uint64_t loop_ns_hist[32];
    uint64_t batch_hist[17];
} OmBusRelayStats;

static inline uint64_t _om_bus_relay_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline uint32_t _om_bus_relay_log2_bin_u64(uint64_t v) {
    if (v == 0) return 0;
    uint32_t b = 0;
    while (v >>= 1U) b++;
    return b;
}

static inline void om_bus_relay_stats_reset(OmBusRelayStats *stats) {
    if (!stats) return;
    memset(stats, 0, sizeof(*stats));
}

static inline uint64_t om_bus_relay_stats_loop_ns_percentile(const OmBusRelayStats *stats,
                                                             uint32_t percentile) {
    if (!stats || stats->loops == 0) return 0;
    if (percentile > 100) percentile = 100;
    uint64_t target = (stats->loops * percentile + 99U) / 100U;
    if (target == 0) target = 1;
    uint64_t acc = 0;
    for (uint32_t i = 0; i < 32; i++) {
        acc += stats->loop_ns_hist[i];
        if (acc >= target) {
            return (i == 0) ? 0 : (1ULL << i);
        }
    }
    return (1ULL << 31);
}

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
    size_t burst_limit = 64;
    OmBusRecord recs[256];

    while (!cfg->running || *cfg->running) {
        uint64_t loop_start_ns = cfg->stats ? _om_bus_relay_now_ns() : 0;
        int rc = om_bus_endpoint_poll_batch(cfg->ep, recs, burst_limit);
        if (rc > 0) {
            om_bus_tcp_server_broadcast_batch(cfg->srv, recs, (uint32_t)rc);
            om_bus_tcp_server_poll_io(cfg->srv);
            idle_spins = 0;
            if ((size_t)rc == burst_limit && burst_limit < 256) {
                burst_limit <<= 1;
            } else if ((size_t)rc * 4 < burst_limit && burst_limit > 16) {
                burst_limit >>= 1;
            }
        } else if (rc == 0) {
            if (idle_spins == 0) {
                om_bus_tcp_server_poll_io(cfg->srv);
            }
            idle_spins++;
            if (idle_spins > 100) {
                usleep(poll_us);
            }
        } else {
            return rc;
        }

        if (cfg->stats) {
            OmBusRelayStats *s = cfg->stats;
            s->loops++;
            if (rc > 0) {
                s->non_empty_loops++;
                s->records_relayed += (uint64_t)rc;
                uint32_t b = (rc >= 16) ? 16U : (uint32_t)rc;
                s->batch_hist[b]++;
            } else {
                s->idle_loops++;
                s->batch_hist[0]++;
            }
            uint64_t loop_ns = _om_bus_relay_now_ns() - loop_start_ns;
            s->loop_ns_total += loop_ns;
            if (loop_ns > s->loop_ns_max) s->loop_ns_max = loop_ns;
            uint32_t bin = _om_bus_relay_log2_bin_u64(loop_ns);
            if (bin > 31U) bin = 31U;
            s->loop_ns_hist[bin]++;
        }
    }

    /* Final flush */
    om_bus_tcp_server_poll_io(cfg->srv);
    return 0;
}

#endif /* OM_BUS_RELAY_H */
