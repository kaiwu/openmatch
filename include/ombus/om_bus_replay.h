#ifndef OM_BUS_REPLAY_H
#define OM_BUS_REPLAY_H

/**
 * @file om_bus_replay.h
 * @brief Header-only helper: replay WAL records into a market worker
 *
 * When a consumer detects OM_ERR_BUS_GAP_DETECTED, it can use this helper
 * to replay the missing WAL range directly from disk into a worker.
 */

#include "openmatch/om_wal.h"
#include "openmarket/om_market.h"

/**
 * Replay WAL records from disk for sequences [from_seq, to_seq).
 *
 * @param wal_path  Path to WAL file (or pattern for multi-file)
 * @param from_seq  First sequence to replay (inclusive)
 * @param to_seq    Last sequence to replay (exclusive), 0 = replay all from from_seq
 * @param w         Private market worker to feed records into
 * @return Number of records replayed, or negative on error
 */
static inline int om_bus_replay_gap(const char *wal_path, uint64_t from_seq,
                                      uint64_t to_seq, OmMarketWorker *w) {
    if (!wal_path || !w) return -1;

    OmWalReplay replay;
    int rc = om_wal_replay_init(&replay, wal_path);
    if (rc != 0) return rc;

    int count = 0;
    OmWalType type;
    void *data;
    uint64_t seq;
    size_t data_len;

    while (om_wal_replay_next(&replay, &type, &data, &seq, &data_len) == 0) {
        if (seq < from_seq) continue;
        if (to_seq > 0 && seq >= to_seq) break;

        int prc = om_market_worker_process(w, type, data);
        if (prc < 0) {
            om_wal_replay_close(&replay);
            return prc;
        }
        count++;
    }

    om_wal_replay_close(&replay);
    return count;
}

/**
 * Replay WAL records into a public market worker.
 * Same as om_bus_replay_gap but for the public (product-level) worker.
 */
static inline int om_bus_replay_gap_public(const char *wal_path, uint64_t from_seq,
                                             uint64_t to_seq, OmMarketPublicWorker *w) {
    if (!wal_path || !w) return -1;

    OmWalReplay replay;
    int rc = om_wal_replay_init(&replay, wal_path);
    if (rc != 0) return rc;

    int count = 0;
    OmWalType type;
    void *data;
    uint64_t seq;
    size_t data_len;

    while (om_wal_replay_next(&replay, &type, &data, &seq, &data_len) == 0) {
        if (seq < from_seq) continue;
        if (to_seq > 0 && seq >= to_seq) break;

        int prc = om_market_public_process(w, type, data);
        if (prc < 0) {
            om_wal_replay_close(&replay);
            return prc;
        }
        count++;
    }

    om_wal_replay_close(&replay);
    return count;
}

#endif /* OM_BUS_REPLAY_H */
