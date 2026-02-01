#ifndef OM_WAL_H
#define OM_WAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* 
 * High-performance WAL optimized for maximum write throughput
 * Design goals: <200ns per write, 5M+ writes/sec per core
 */

/* WAL record types */
typedef enum OmWalType {
    OM_WAL_INSERT = 1,      /* 64 bytes */
    OM_WAL_CANCEL = 2,      /* 32 bytes */
    OM_WAL_MATCH = 3,       /* 48 bytes */
    OM_WAL_CHECKPOINT = 4,  /* 32 bytes */
} OmWalType;

/* Compact record header - 8 bytes only */
typedef struct OmWalHeader {
    uint64_t seq_type_len;  /* Packed: sequence (40 bits) | type (8 bits) | len (16 bits) */
} OmWalHeader;

/* Insert record - total 64 bytes (header + payload) */
typedef struct OmWalInsert {
    uint64_t order_id;      /* 8 bytes */
    uint64_t price;         /* 8 bytes */
    uint64_t volume;        /* 8 bytes */
    uint64_t vol_remain;    /* 8 bytes */
    uint32_t slot_idx;      /* 4 bytes */
    uint16_t product_id;    /* 2 bytes */
    uint16_t org;           /* 2 bytes */
    uint16_t flags;         /* 2 bytes */
    uint16_t reserved[3];   /* 6 bytes - padding to 48 byte payload */
    /* Header: 8 bytes, Payload: 48 bytes, Total: 56 bytes - add 8 more for alignment */
    uint64_t timestamp_ns;  /* 8 bytes - total 64 bytes */
} OmWalInsert;

/* Cancel record - total 32 bytes */
typedef struct OmWalCancel {
    uint64_t order_id;      /* 8 bytes */
    uint64_t timestamp_ns;  /* 8 bytes */
    uint32_t slot_idx;      /* 4 bytes */
    uint16_t product_id;    /* 2 bytes */
    uint16_t reserved;      /* 2 bytes */
    /* Total payload: 24 bytes + 8 byte header = 32 bytes */
} OmWalCancel;

/* Match record - total 48 bytes */
typedef struct OmWalMatch {
    uint64_t maker_id;      /* 8 bytes */
    uint64_t taker_id;      /* 8 bytes */
    uint64_t price;         /* 8 bytes */
    uint64_t volume;        /* 8 bytes */
    uint64_t timestamp_ns;  /* 8 bytes */
    uint16_t product_id;    /* 2 bytes */
    uint16_t reserved[3];   /* 6 bytes - total payload: 40 bytes + 8 header = 48 */
} OmWalMatch;

/* WAL configuration for maximum throughput */
typedef struct OmWalConfig {
    const char *filename;       /* WAL file path */
    size_t buffer_size;         /* Per-thread buffer size (default 1MB) */
    uint32_t batch_size;        /* Records to batch before write (default 100) */
    uint32_t sync_interval_ms;  /* Fsync interval in ms (default 10) */
    bool use_direct_io;         /* Use O_DIRECT (default true) */
    bool enable_crc32;          /* CRC32 validation (default false for speed) */
} OmWalConfig;

/* WAL context */
typedef struct OmWal {
    int fd;                     /* File descriptor (O_DIRECT if enabled) */
    void *buffer;               /* Write buffer (aligned for O_DIRECT) */
    void *buffer_unaligned;     /* Original malloc pointer for freeing */
    size_t buffer_size;         /* Total buffer size */
    size_t buffer_used;         /* Bytes used in current buffer */
    uint64_t sequence;          /* Next sequence number */
    uint64_t file_offset;       /* Current file offset */
    OmWalConfig config;         /* Configuration copy */
} OmWal;

/* Initialize WAL with high-performance settings */
int om_wal_init(OmWal *wal, const OmWalConfig *config);

/* Close WAL and free resources */
void om_wal_close(OmWal *wal);

/* Write operations - all return sequence number on success, 0 on failure */
/* These are FAST PATH - just append to buffer, no syscalls, no locks */
uint64_t om_wal_insert(OmWal *wal, const OmWalInsert *rec);
uint64_t om_wal_cancel(OmWal *wal, const OmWalCancel *rec);
uint64_t om_wal_match(OmWal *wal, const OmWalMatch *rec);

/* Flush buffer to disk - call periodically or when buffer is full */
/* This is where the actual write() syscall happens */
int om_wal_flush(OmWal *wal);

/* Force fsync - call on checkpoint or graceful shutdown */
int om_wal_fsync(OmWal *wal);

/* Batch write multiple records at once - more efficient */
uint64_t om_wal_write_batch(OmWal *wal, OmWalType type, const void *records, 
                            uint32_t count, size_t record_size);

/* Get current sequence number */
static inline uint64_t om_wal_sequence(const OmWal *wal) {
    return wal ? wal->sequence : 0;
}

/* Calculate packed header value */
static inline uint64_t om_wal_pack_header(uint64_t seq, uint8_t type, uint16_t len) {
    return (seq << 24) | ((uint64_t)type << 16) | len;
}

/* Extract from packed header */
static inline uint64_t om_wal_header_seq(uint64_t packed) { return packed >> 24; }
static inline uint8_t om_wal_header_type(uint64_t packed) { return (packed >> 16) & 0xFF; }
static inline uint16_t om_wal_header_len(uint64_t packed) { return packed & 0xFFFF; }

/* ============================================================================
 * WAL REPLAY / RECOVERY
 * ============================================================================ */

/* Replay iterator for scanning WAL files */
typedef struct OmWalReplay {
    int fd;                     /* File descriptor for reading */
    void *buffer;               /* Read buffer (4KB aligned) */
    void *buffer_unaligned;     /* Original allocation pointer */
    size_t buffer_size;         /* Buffer size (typically 1MB) */
    size_t buffer_valid;        /* Valid bytes in buffer */
    size_t buffer_pos;          /* Current read position in buffer */
    uint64_t file_offset;       /* Current offset in file */
    uint64_t file_size;         /* Total file size */
    uint64_t last_sequence;     /* Last sequence number read */
    bool eof;                   /* End of file reached */
} OmWalReplay;

/* Initialize replay iterator for reading WAL file */
int om_wal_replay_init(OmWalReplay *replay, const char *filename);

/* Close replay iterator */
void om_wal_replay_close(OmWalReplay *replay);

/* Read next record from WAL during replay */
/* Returns: 1 = success, 0 = EOF, -1 = error */
int om_wal_replay_next(OmWalReplay *replay, OmWalType *type, void **data, 
                       uint64_t *sequence, size_t *data_len);

/* Get statistics about replay progress */
typedef struct OmWalReplayStats {
    uint64_t records_insert;
    uint64_t records_cancel;
    uint64_t records_match;
    uint64_t records_other;
    uint64_t bytes_processed;
    uint64_t last_sequence;
} OmWalReplayStats;

/* Forward declaration - defined in orderbook.h */
struct OmOrderbookContext;

/**
 * Replay WAL file and reconstruct orderbook state
 * This is the main recovery function - call on startup to restore state after crash
 * 
 * @param ctx Orderbook context to reconstruct
 * @param wal_filename Path to WAL file
 * @param stats Output statistics (can be NULL)
 * @return 0 on success, negative on error
 * 
 * Process:
 * 1. Scan WAL file from beginning
 * 2. For INSERT: allocate slot, restore order data, add to price ladder and hashmap
 * 3. For CANCEL: remove order from price ladder, remove from hashmap, free slot
 * 4. For MATCH: update volume_remain, if fully filled remove order
 * 5. Rebuild all indices (price ladders, hashmap, etc.)
 */
int om_orderbook_recover_from_wal(struct OmOrderbookContext *ctx, 
                                   const char *wal_filename,
                                   OmWalReplayStats *stats);

#endif
