#ifndef OM_WAL_MOCK_H
#define OM_WAL_MOCK_H

/*
 * WAL Mocker - Debug/Development Version
 * 
 * This module provides the exact same interface as om_wal.h but instead of
 * writing to disk, it prints human-readable WAL operations to stderr.
 * 
 * Use this for:
 * - Debugging orderbook operations
 * - Development without file I/O overhead
 * - Unit testing WAL integration
 * - Visualizing what would be persisted
 * 
 * To use: Simply include this header instead of om_wal.h, or use the
 * om_wal_mock_ prefixed functions which have the same signatures.
 */

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "om_slab.h"

/* ============================================================================
 * Types - Mirror of om_wal.h exactly
 * ============================================================================ */

typedef enum OmWalType {
    OM_WAL_INSERT = 1,
    OM_WAL_CANCEL = 2,
    OM_WAL_MATCH = 3,
    OM_WAL_CHECKPOINT = 4,
    OM_WAL_DEACTIVATE = 5,
    OM_WAL_ACTIVATE = 6,
    OM_WAL_USER_BASE = 0x80
} OmWalType;

typedef struct OmWalHeader {
    uint64_t seq_type_len;
} OmWalHeader;

typedef struct OmWalInsert {
    uint64_t order_id;
    uint64_t price;
    uint64_t volume;
    uint64_t vol_remain;
    uint16_t org;
    uint16_t flags;
    uint16_t product_id;
    uint16_t reserved;
    uint32_t user_data_size;
    uint32_t aux_data_size;
    uint64_t timestamp_ns;
} OmWalInsert;

typedef struct OmWalCancel {
    uint64_t order_id;
    uint64_t timestamp_ns;
    uint32_t slot_idx;
    uint16_t product_id;
    uint16_t reserved;
} OmWalCancel;

typedef struct OmWalMatch {
    uint64_t maker_id;
    uint64_t taker_id;
    uint64_t price;
    uint64_t volume;
    uint64_t timestamp_ns;
    uint16_t product_id;
    uint16_t reserved[3];
} OmWalMatch;

typedef struct OmWalDeactivate {
    uint64_t order_id;
    uint64_t timestamp_ns;
    uint32_t slot_idx;
    uint16_t product_id;
    uint16_t reserved;
} OmWalDeactivate;

typedef struct OmWalActivate {
    uint64_t order_id;
    uint64_t timestamp_ns;
    uint32_t slot_idx;
    uint16_t product_id;
    uint16_t reserved;
} OmWalActivate;

typedef struct OmWalConfig {
    const char *filename;       /* Ignored in mock */
    size_t buffer_size;         /* Ignored in mock */
    uint32_t sync_interval_ms;  /* Ignored in mock */
    bool use_direct_io;         /* Ignored in mock */
    bool enable_crc32;          /* Ignored in mock */
    size_t user_data_size;
    size_t aux_data_size;
} OmWalConfig;

typedef struct OmWal {
    /* Mock state - tracks sequence numbers and stats */
    uint64_t sequence;
    uint64_t inserts_logged;
    uint64_t cancels_logged;
    uint64_t matches_logged;
    uint64_t deactivates_logged;
    uint64_t activates_logged;
    bool enabled;               /* Can disable output */
    bool show_timestamp;        /* Show timestamps */
    bool show_aux_data;         /* Show hex dump of aux data */
    size_t user_data_size;
    size_t aux_data_size;

    /* Post-write callback - invoked after each WAL write with record data */
    void (*post_write)(uint64_t seq, uint8_t type, const void *data,
                       uint16_t len, void *ctx);
    void *post_write_ctx;
} OmWal;

typedef struct OmWalReplay {
    /* Mock replay - always returns EOF */
    bool eof;
} OmWalReplay;

typedef struct OmWalReplayStats {
    uint64_t records_insert;
    uint64_t records_cancel;
    uint64_t records_match;
    uint64_t records_other;
    uint64_t bytes_processed;
    uint64_t last_sequence;
} OmWalReplayStats;

struct OmOrderbookContext;

/* ============================================================================
 * API - Exact same interface as om_wal.h
 * ============================================================================ */

/* Initialize mock WAL - prints initialization message */
int om_wal_mock_init(OmWal *wal, const OmWalConfig *config);

/* Close mock WAL - prints summary */
void om_wal_mock_close(OmWal *wal);

/* Set slab pointer for aux data access (mock no-op) */
static inline void om_wal_mock_set_slab(OmWal *wal, OmDualSlab *slab) {
    (void)wal;
    (void)slab;
}

/* Set post-write callback */
static inline void om_wal_mock_set_post_write(OmWal *wal,
    void (*fn)(uint64_t, uint8_t, const void*, uint16_t, void*), void *ctx) {
    if (wal) {
        wal->post_write = fn;
        wal->post_write_ctx = ctx;
    }
}

/* Log insert - prints INSERT operation to stderr */
uint64_t om_wal_mock_insert(OmWal *wal, struct OmSlabSlot *slot, uint16_t product_id);

/* Log cancel - prints CANCEL operation to stderr */
uint64_t om_wal_mock_cancel(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id);

/* Log match - prints MATCH operation to stderr */
uint64_t om_wal_mock_match(OmWal *wal, const OmWalMatch *rec);

/* Log deactivate - prints DEACTIVATE operation to stderr */
uint64_t om_wal_mock_deactivate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id);

/* Log activate - prints ACTIVATE operation to stderr */
uint64_t om_wal_mock_activate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id);

/* Flush - prints FLUSH message */
int om_wal_mock_flush(OmWal *wal);

/* Fsync - prints FSYNC message */
int om_wal_mock_fsync(OmWal *wal);

/* Get sequence - returns current sequence number */
static inline uint64_t om_wal_mock_sequence(const OmWal *wal) {
    return wal ? wal->sequence : 0;
}

/* Pack/unpack helpers - same as real WAL */
static inline uint64_t om_wal_mock_pack_header(uint64_t seq, uint8_t type, uint16_t len) {
    return (seq << 24) | ((uint64_t)type << 16) | len;
}
static inline uint64_t om_wal_mock_header_seq(uint64_t packed) { return packed >> 24; }
static inline uint8_t om_wal_mock_header_type(uint64_t packed) { return (packed >> 16) & 0xFF; }
static inline uint16_t om_wal_mock_header_len(uint64_t packed) { return packed & 0xFFFF; }

/* Replay functions - mock returns EOF immediately */
int om_wal_mock_replay_init(OmWalReplay *replay, const char *filename);
int om_wal_mock_replay_init_with_sizes(OmWalReplay *replay, const char *filename,
                                       size_t user_data_size, size_t aux_data_size);
int om_wal_mock_replay_init_with_config(OmWalReplay *replay, const char *filename,
                                        const OmWalConfig *config);
void om_wal_mock_replay_close(OmWalReplay *replay);
int om_wal_mock_replay_next(OmWalReplay *replay, OmWalType *type, void **data, 
                            uint64_t *sequence, size_t *data_len);

/* Recovery - mock always succeeds with 0 records */
int om_wal_mock_recover_from_wal(struct OmOrderbookContext *ctx, 
                                  const char *wal_filename,
                                  OmWalReplayStats *stats);

/* ============================================================================
 * Compatibility Aliases - Use these to swap with real WAL
 * ============================================================================ */

/* These aliases allow you to swap between mock and real by changing include */
#define om_wal_init         om_wal_mock_init
#define om_wal_close        om_wal_mock_close
#define om_wal_set_slab     om_wal_mock_set_slab
#define om_wal_set_post_write om_wal_mock_set_post_write
#define om_wal_insert       om_wal_mock_insert
#define om_wal_cancel       om_wal_mock_cancel
#define om_wal_match        om_wal_mock_match
#define om_wal_deactivate   om_wal_mock_deactivate
#define om_wal_activate     om_wal_mock_activate
#define om_wal_append_custom om_wal_mock_append_custom
#define om_wal_flush        om_wal_mock_flush
#define om_wal_fsync        om_wal_mock_fsync
#define om_wal_sequence     om_wal_mock_sequence
#define om_wal_pack_header  om_wal_mock_pack_header
#define om_wal_header_seq   om_wal_mock_header_seq
#define om_wal_header_type  om_wal_mock_header_type
#define om_wal_header_len   om_wal_mock_header_len
#define om_wal_replay_init      om_wal_mock_replay_init
#define om_wal_replay_init_with_sizes om_wal_mock_replay_init_with_sizes
#define om_wal_replay_init_with_config om_wal_mock_replay_init_with_config
#define om_wal_replay_close     om_wal_mock_replay_close
#define om_wal_replay_next      om_wal_mock_replay_next
#define om_orderbook_recover_from_wal om_wal_mock_recover_from_wal

/* ============================================================================
 * Debug Controls
 * ============================================================================ */

/* Enable/disable output */
static inline void om_wal_mock_set_enabled(OmWal *wal, bool enabled) {
    if (wal) wal->enabled = enabled;
}

/* Toggle timestamp display */
static inline void om_wal_mock_set_show_timestamp(OmWal *wal, bool show) {
    if (wal) wal->show_timestamp = show;
}

/* Toggle aux data hex dump */
static inline void om_wal_mock_set_show_aux_data(OmWal *wal, bool show) {
    if (wal) wal->show_aux_data = show;
}

/* Print formatted WAL stats */
void om_wal_mock_print_stats(const OmWal *wal);

uint64_t om_wal_mock_append_custom(OmWal *wal, OmWalType type, const void *data, size_t len);


#endif
