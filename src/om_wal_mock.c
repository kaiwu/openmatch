#include "openmatch/om_wal_mock.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

static uint64_t wal_mock_now_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return 0;
    }
    return ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
}

int om_wal_mock_init(OmWal *wal, const OmWalConfig *config) {
    if (!wal) {
        return -1;
    }
    memset(wal, 0, sizeof(*wal));
    wal->enabled = true;
    wal->show_timestamp = true;
    if (config) {
        wal->user_data_size = config->user_data_size;
        wal->aux_data_size = config->aux_data_size;
    }
    fprintf(stderr, "WAL MOCK init\n");
    return 0;
}

void om_wal_mock_close(OmWal *wal) {
    if (!wal) {
        return;
    }
    fprintf(stderr, "WAL MOCK close: inserts=%" PRIu64 " cancels=%" PRIu64
                    " matches=%" PRIu64 " deact=%" PRIu64 " act=%" PRIu64 "\n",
            wal->inserts_logged, wal->cancels_logged, wal->matches_logged,
            wal->deactivates_logged, wal->activates_logged);
}

uint64_t om_wal_mock_insert(OmWal *wal, struct OmSlabSlot *slot, uint16_t product_id) {
    if (!wal || !slot) {
        return 0;
    }
    wal->sequence++;
    wal->inserts_logged++;
    if (wal->enabled) {
        uint64_t ts = wal->show_timestamp ? wal_mock_now_ns() : 0;
        fprintf(stderr, "seq[%" PRIu64 "] type[INSERT] oid[%" PRIu32 "] p[%" PRIu64 "] v[%" PRIu64
                        "] vr[%" PRIu64 "] org[%" PRIu16 "] f[0x%04" PRIx16 "] pid[%" PRIu16
                        "] ts[%" PRIu64 "]\n",
                wal->sequence, slot->order_id, slot->price, slot->volume,
                slot->volume_remain, slot->org, slot->flags, product_id, ts);
    }
    return wal->sequence;
}

uint64_t om_wal_mock_cancel(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id) {
    if (!wal) {
        return 0;
    }
    wal->sequence++;
    wal->cancels_logged++;
    if (wal->enabled) {
        uint64_t ts = wal->show_timestamp ? wal_mock_now_ns() : 0;
        fprintf(stderr, "seq[%" PRIu64 "] type[CANCEL] oid[%" PRIu32 "] s[%" PRIu32
                        "] pid[%" PRIu16 "] ts[%" PRIu64 "]\n",
                wal->sequence, order_id, slot_idx, product_id, ts);
    }
    return wal->sequence;
}

uint64_t om_wal_mock_match(OmWal *wal, const OmWalMatch *rec) {
    if (!wal || !rec) {
        return 0;
    }
    wal->sequence++;
    wal->matches_logged++;
    if (wal->enabled) {
        fprintf(stderr, "seq[%" PRIu64 "] type[MATCH] m[%" PRIu64 "] t[%" PRIu64
                        "] p[%" PRIu64 "] q[%" PRIu64 "] pid[%" PRIu16 "] ts[%" PRIu64 "]\n",
                wal->sequence, rec->maker_id, rec->taker_id, rec->price, rec->volume,
                rec->product_id, rec->timestamp_ns);
    }
    return wal->sequence;
}

uint64_t om_wal_mock_deactivate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id) {
    if (!wal) {
        return 0;
    }
    wal->sequence++;
    wal->deactivates_logged++;
    if (wal->enabled) {
        uint64_t ts = wal->show_timestamp ? wal_mock_now_ns() : 0;
        fprintf(stderr, "seq[%" PRIu64 "] type[DEACTIVATE] oid[%" PRIu32 "] s[%" PRIu32
                        "] pid[%" PRIu16 "] ts[%" PRIu64 "]\n",
                wal->sequence, order_id, slot_idx, product_id, ts);
    }
    return wal->sequence;
}

uint64_t om_wal_mock_activate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id) {
    if (!wal) {
        return 0;
    }
    wal->sequence++;
    wal->activates_logged++;
    if (wal->enabled) {
        uint64_t ts = wal->show_timestamp ? wal_mock_now_ns() : 0;
        fprintf(stderr, "seq[%" PRIu64 "] type[ACTIVATE] oid[%" PRIu32 "] s[%" PRIu32
                        "] pid[%" PRIu16 "] ts[%" PRIu64 "]\n",
                wal->sequence, order_id, slot_idx, product_id, ts);
    }
    return wal->sequence;
}

int om_wal_mock_flush(OmWal *wal) {
    if (wal && wal->enabled) {
        fprintf(stderr, "WAL MOCK FLUSH\n");
    }
    return 0;
}

int om_wal_mock_fsync(OmWal *wal) {
    if (wal && wal->enabled) {
        fprintf(stderr, "WAL MOCK FSYNC\n");
    }
    return 0;
}

int om_wal_mock_replay_init(OmWalReplay *replay, const char *filename) {
    (void)filename;
    if (!replay) {
        return -1;
    }
    replay->eof = true;
    return 0;
}

int om_wal_mock_replay_init_with_sizes(OmWalReplay *replay, const char *filename,
                                       size_t user_data_size, size_t aux_data_size) {
    (void)user_data_size;
    (void)aux_data_size;
    return om_wal_mock_replay_init(replay, filename);
}

int om_wal_mock_replay_init_with_config(OmWalReplay *replay, const char *filename,
                                        const OmWalConfig *config) {
    (void)config;
    return om_wal_mock_replay_init(replay, filename);
}

void om_wal_mock_replay_close(OmWalReplay *replay) {
    if (replay) {
        replay->eof = true;
    }
}

int om_wal_mock_replay_next(OmWalReplay *replay, OmWalType *type, void **data,
                            uint64_t *sequence, size_t *data_len) {
    (void)replay;
    (void)type;
    (void)data;
    (void)sequence;
    (void)data_len;
    return 0;
}

int om_wal_mock_recover_from_wal(struct OmOrderbookContext *ctx,
                                 const char *wal_filename,
                                 OmWalReplayStats *stats) {
    (void)ctx;
    (void)wal_filename;
    if (stats) {
        memset(stats, 0, sizeof(*stats));
    }
    return 0;
}

void om_wal_mock_print_stats(const OmWal *wal) {
    if (!wal) {
        return;
    }
    fprintf(stderr, "WAL MOCK stats: inserts=%" PRIu64 " cancels=%" PRIu64
                    " matches=%" PRIu64 " deact=%" PRIu64 " act=%" PRIu64 "\n",
            wal->inserts_logged, wal->cancels_logged, wal->matches_logged,
            wal->deactivates_logged, wal->activates_logged);
}

uint64_t om_wal_mock_append_custom(OmWal *wal, OmWalType type, const void *data, size_t len) {
    if (!wal || !data) {
        return 0;
    }
    if (type < (OmWalType)OM_WAL_USER_BASE) {
        return 0;
    }
    wal->sequence++;
    if (wal->enabled) {
        fprintf(stderr, "seq[%" PRIu64 "] type[USER] ut[%u] len[%zu]\n",
                wal->sequence, (unsigned)type, len);
    }
    return wal->sequence;
}
