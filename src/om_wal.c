#define _GNU_SOURCE  /* For O_DIRECT */
#include "om_wal.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <assert.h>

/* Align to 4KB for O_DIRECT */
#define WAL_ALIGN 4096
#define WAL_ALIGN_MASK (WAL_ALIGN - 1)

/* Record sizes - must be power of 2 for alignment */
#define WAL_INSERT_SIZE 64
#define WAL_CANCEL_SIZE 32
#define WAL_MATCH_SIZE 48
#define WAL_HEADER_SIZE 8

/* Align pointer up to boundary */
static inline void *align_up(void *ptr, size_t align) {
    uintptr_t p = (uintptr_t)ptr;
    return (void *)((p + align - 1) & ~(align - 1));
}

/* Get record size based on type */
static inline size_t wal_record_size(OmWalType type) {
    switch (type) {
        case OM_WAL_INSERT: return WAL_INSERT_SIZE;
        case OM_WAL_CANCEL: return WAL_CANCEL_SIZE;
        case OM_WAL_MATCH: return WAL_MATCH_SIZE;
        case OM_WAL_CHECKPOINT: return 32;
        default: return 0;
    }
}

int om_wal_init(OmWal *wal, const OmWalConfig *config) {
    if (!wal || !config || !config->filename) {
        return -1;
    }

    memset(wal, 0, sizeof(OmWal));
    wal->config = *config;

    /* Set defaults for high performance */
    if (wal->config.buffer_size == 0) {
        wal->config.buffer_size = 1024 * 1024;  /* 1MB default */
    }
    /* Align buffer size to 4KB */
    wal->config.buffer_size = (wal->config.buffer_size + WAL_ALIGN - 1) & ~WAL_ALIGN_MASK;

    /* Allocate aligned buffer for O_DIRECT */
    wal->buffer_unaligned = malloc(wal->config.buffer_size + WAL_ALIGN);
    if (!wal->buffer_unaligned) {
        return -1;
    }
    wal->buffer = align_up(wal->buffer_unaligned, WAL_ALIGN);
    wal->buffer_size = wal->config.buffer_size;
    wal->buffer_used = 0;

    /* Open file with O_DIRECT for maximum throughput */
    int flags = O_WRONLY | O_CREAT | O_APPEND;
    if (wal->config.use_direct_io) {
        flags |= O_DIRECT;
    }

    wal->fd = open(config->filename, flags, 0644);
    if (wal->fd < 0) {
        free(wal->buffer_unaligned);
        return -1;
    }

    /* Get current file size to determine sequence */
    struct stat st;
    if (fstat(wal->fd, &st) == 0) {
        wal->file_offset = st.st_size;
        /* TODO: Parse existing records to find next sequence */
        wal->sequence = 1;
    } else {
        wal->sequence = 1;
    }

    return 0;
}

void om_wal_close(OmWal *wal) {
    if (!wal) return;

    /* Flush remaining buffer */
    if (wal->buffer_used > 0) {
        om_wal_flush(wal);
    }

    /* Final fsync */
    if (wal->fd >= 0) {
        fsync(wal->fd);
        close(wal->fd);
        wal->fd = -1;
    }

    if (wal->buffer_unaligned) {
        free(wal->buffer_unaligned);
        wal->buffer_unaligned = NULL;
        wal->buffer = NULL;
    }
}

/* FAST PATH: Append record to buffer, no syscalls */
static uint64_t wal_append(OmWal *wal, OmWalType type, const void *data, size_t data_size) {
    size_t total_size = WAL_HEADER_SIZE + data_size;
    
    /* Check if we need to flush */
    if (wal->buffer_used + total_size > wal->buffer_size) {
        if (om_wal_flush(wal) != 0) {
            return 0;
        }
    }

    /* Get next sequence number */
    uint64_t seq = wal->sequence++;

    /* Write header (packed 8 bytes) */
    uint64_t header = om_wal_pack_header(seq, type, data_size);
    memcpy((char *)wal->buffer + wal->buffer_used, &header, WAL_HEADER_SIZE);
    wal->buffer_used += WAL_HEADER_SIZE;

    /* Write payload */
    memcpy((char *)wal->buffer + wal->buffer_used, data, data_size);
    wal->buffer_used += data_size;

    return seq;
}

uint64_t om_wal_insert(OmWal *wal, const OmWalInsert *rec) {
    return wal_append(wal, OM_WAL_INSERT, rec, sizeof(OmWalInsert));
}

uint64_t om_wal_cancel(OmWal *wal, const OmWalCancel *rec) {
    return wal_append(wal, OM_WAL_CANCEL, rec, sizeof(OmWalCancel));
}

uint64_t om_wal_match(OmWal *wal, const OmWalMatch *rec) {
    return wal_append(wal, OM_WAL_MATCH, rec, sizeof(OmWalMatch));
}

/* Write buffer to disk - this is the only syscall in hot path */
int om_wal_flush(OmWal *wal) {
    if (wal->buffer_used == 0) {
        return 0;
    }

    /* Align write size to 4KB for O_DIRECT */
    size_t write_size = (wal->buffer_used + WAL_ALIGN - 1) & ~WAL_ALIGN_MASK;
    
    /* Zero-pad to alignment boundary */
    if (write_size > wal->buffer_used) {
        memset((char *)wal->buffer + wal->buffer_used, 0, 
               write_size - wal->buffer_used);
    }

    /* Write to file */
    ssize_t written = write(wal->fd, wal->buffer, write_size);
    if (written != (ssize_t)write_size) {
        return -1;
    }

    wal->file_offset += write_size;
    wal->buffer_used = 0;

    return 0;
}

/* Force fsync for durability */
int om_wal_fsync(OmWal *wal) {
    if (wal->buffer_used > 0) {
        if (om_wal_flush(wal) != 0) {
            return -1;
        }
    }

    if (fsync(wal->fd) != 0) {
        return -1;
    }

    return 0;
}

/* Batch write multiple records - more efficient for bulk operations */
uint64_t om_wal_write_batch(OmWal *wal, OmWalType type, const void *records, 
                            uint32_t count, size_t record_size) {
    if (!wal || !records || count == 0) {
        return 0;
    }

    size_t batch_size = count * (WAL_HEADER_SIZE + record_size);
    
    /* Check if batch fits in buffer */
    if (batch_size > wal->buffer_size) {
        /* Too large for buffer, flush first and write directly */
        if (om_wal_flush(wal) != 0) {
            return 0;
        }
        /* TODO: Write directly to avoid double copy */
    }

    uint64_t first_seq = wal->sequence;
    const char *rec_ptr = records;

    for (uint32_t i = 0; i < count; i++) {
        uint64_t seq = wal_append(wal, type, rec_ptr, record_size);
        if (seq == 0) {
            return 0;
        }
        rec_ptr += record_size;
    }

    return first_seq;
}

/* ============================================================================
 * WAL REPLAY / RECOVERY IMPLEMENTATION
 * ============================================================================ */

#define REPLAY_BUFFER_SIZE (1024 * 1024)  /* 1MB read buffer */
#define REPLAY_ALIGN 4096

int om_wal_replay_init(OmWalReplay *replay, const char *filename) {
    if (!replay || !filename) {
        return -1;
    }

    memset(replay, 0, sizeof(OmWalReplay));

    /* Open file for reading (without O_DIRECT for simplicity) */
    replay->fd = open(filename, O_RDONLY);
    if (replay->fd < 0) {
        return -1;
    }

    /* Get file size */
    struct stat st;
    if (fstat(replay->fd, &st) != 0) {
        close(replay->fd);
        replay->fd = -1;
        return -1;
    }
    replay->file_size = st.st_size;

    /* Allocate aligned buffer */
    replay->buffer_unaligned = malloc(REPLAY_BUFFER_SIZE + REPLAY_ALIGN);
    if (!replay->buffer_unaligned) {
        close(replay->fd);
        replay->fd = -1;
        return -1;
    }
    replay->buffer = align_up(replay->buffer_unaligned, REPLAY_ALIGN);
    replay->buffer_size = REPLAY_BUFFER_SIZE;
    replay->buffer_valid = 0;
    replay->buffer_pos = 0;
    replay->file_offset = 0;
    replay->last_sequence = 0;
    replay->eof = false;

    return 0;
}

void om_wal_replay_close(OmWalReplay *replay) {
    if (!replay) return;

    if (replay->fd >= 0) {
        close(replay->fd);
        replay->fd = -1;
    }

    if (replay->buffer_unaligned) {
        free(replay->buffer_unaligned);
        replay->buffer_unaligned = NULL;
        replay->buffer = NULL;
    }

    replay->buffer_valid = 0;
    replay->buffer_pos = 0;
}

/* Fill buffer from file */
static int replay_fill_buffer(OmWalReplay *replay) {
    if (replay->eof || replay->file_offset >= replay->file_size) {
        return 0;
    }

    /* Move remaining data to beginning of buffer */
    size_t remaining = replay->buffer_valid - replay->buffer_pos;
    if (remaining > 0) {
        memmove(replay->buffer, 
                (char *)replay->buffer + replay->buffer_pos, 
                remaining);
    }

    /* Read more data */
    size_t to_read = replay->buffer_size - remaining;
    size_t space_left = replay->file_size - replay->file_offset;
    if (to_read > space_left) {
        to_read = space_left;
    }

    ssize_t n = read(replay->fd, (char *)replay->buffer + remaining, to_read);
    if (n < 0) {
        return -1;
    }
    if (n == 0) {
        replay->eof = true;
        return 0;
    }

    replay->buffer_valid = remaining + n;
    replay->buffer_pos = 0;
    replay->file_offset += n;

    return 1;
}

int om_wal_replay_next(OmWalReplay *replay, OmWalType *type, void **data, 
                       uint64_t *sequence, size_t *data_len) {
    if (!replay || !type || !data || !sequence || !data_len) {
        return -1;
    }

    /* Check if we need to refill buffer */
    if (replay->buffer_pos + sizeof(OmWalHeader) > replay->buffer_valid) {
        int ret = replay_fill_buffer(replay);
        if (ret < 0) return -1;
        if (ret == 0) return 0;  /* EOF */
        
        /* Still not enough data? */
        if (replay->buffer_pos + sizeof(OmWalHeader) > replay->buffer_valid) {
            return 0;  /* Partial record at end - treat as EOF */
        }
    }

    /* Read header */
    OmWalHeader *header = (OmWalHeader *)((char *)replay->buffer + replay->buffer_pos);
    uint64_t packed = header->seq_type_len;
    *sequence = om_wal_header_seq(packed);
    *type = (OmWalType)om_wal_header_type(packed);
    *data_len = om_wal_header_len(packed);

    replay->buffer_pos += sizeof(OmWalHeader);

    /* Check if payload is in buffer */
    if (replay->buffer_pos + *data_len > replay->buffer_valid) {
        int ret = replay_fill_buffer(replay);
        if (ret < 0) return -1;
        if (ret == 0 || replay->buffer_pos + *data_len > replay->buffer_valid) {
            return -1;  /* Corrupted/truncated record */
        }
    }

    /* Return pointer to data */
    *data = (char *)replay->buffer + replay->buffer_pos;
    replay->buffer_pos += *data_len;
    replay->last_sequence = *sequence;

    return 1;
}
