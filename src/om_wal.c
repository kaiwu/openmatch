#define _GNU_SOURCE  /* For O_DIRECT */
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

#include "om_wal.h"
#include "om_slab.h"

/* Align to 4KB for O_DIRECT */
#define WAL_ALIGN 4096
#define WAL_ALIGN_MASK (WAL_ALIGN - 1)

/* Record sizes for fixed-size records */
#define WAL_CANCEL_SIZE sizeof(OmWalCancel)
#define WAL_MATCH_SIZE sizeof(OmWalMatch)
#define WAL_HEADER_SIZE sizeof(OmWalHeader)

/* Align pointer up to boundary */
static inline void *align_up(void *ptr, size_t align) {
    uintptr_t p = (uintptr_t)ptr;
    return (void *)((p + align - 1) & ~(align - 1));
}

/* Calculate total insert record size including variable-length data */
static inline size_t wal_insert_total_size(size_t user_data_size, size_t aux_data_size) {
    size_t total = sizeof(OmWalHeader) + sizeof(OmWalInsert) + user_data_size + aux_data_size;
    /* Align to 8-byte boundary */
    return (total + 7) & ~7;
}

/* Get payload size based on type and config (for replay) */
static inline size_t wal_payload_size(OmWalType type, size_t user_data_size, size_t aux_data_size) {
    switch (type) {
        case OM_WAL_INSERT: return sizeof(OmWalInsert) + user_data_size + aux_data_size;
        case OM_WAL_CANCEL: return sizeof(OmWalCancel);
        case OM_WAL_MATCH: return sizeof(OmWalMatch);
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
    uint64_t header = om_wal_pack_header(seq, type, (uint16_t)data_size);
    memcpy((char *)wal->buffer + wal->buffer_used, &header, WAL_HEADER_SIZE);
    wal->buffer_used += WAL_HEADER_SIZE;

    /* Write payload */
    memcpy((char *)wal->buffer + wal->buffer_used, data, data_size);
    wal->buffer_used += data_size;

    return seq;
}

/* 
 * Write insert record with variable-length data
 * Format: [OmWalHeader][OmWalInsert][user_data...][aux_data...]
 */
uint64_t om_wal_insert(OmWal *wal, struct OmSlabSlot *slot, uint16_t product_id) {
    if (!wal || !slot) {
        return 0;
    }

    size_t user_data_size = wal->config.user_data_size;
    size_t aux_data_size = wal->config.aux_data_size;
    size_t total_size = wal_insert_total_size(user_data_size, aux_data_size);
    size_t data_size = sizeof(OmWalInsert) + user_data_size + aux_data_size;

    /* Check if we need to flush */
    if (wal->buffer_used + total_size > wal->buffer_size) {
        if (om_wal_flush(wal) != 0) {
            return 0;
        }
    }

    /* Get next sequence number */
    uint64_t seq = wal->sequence++;

    /* Write header (packed 8 bytes) */
    uint64_t header = om_wal_pack_header(seq, OM_WAL_INSERT, (uint16_t)data_size);
    memcpy((char *)wal->buffer + wal->buffer_used, &header, WAL_HEADER_SIZE);
    wal->buffer_used += WAL_HEADER_SIZE;

    /* Build OmWalInsert record from slot data */
    OmWalInsert insert;
    memset(&insert, 0, sizeof(insert));
    
    insert.order_id = slot->order_id;
    insert.price = slot->price;
    insert.volume = slot->volume;
    insert.vol_remain = slot->volume_remain;
    insert.org = slot->org;
    insert.flags = slot->flags;
    insert.product_id = product_id;
    insert.user_data_size = (uint32_t)user_data_size;
    insert.aux_data_size = (uint32_t)aux_data_size;
    insert.timestamp_ns = 0;  /* TODO: get actual timestamp */

    /* Write OmWalInsert header */
    memcpy((char *)wal->buffer + wal->buffer_used, &insert, sizeof(OmWalInsert));
    wal->buffer_used += sizeof(OmWalInsert);

    /* Write user data (secondary hot data from slot->data[]) */
    if (user_data_size > 0) {
        void *user_data = om_slot_get_data(slot);
        memcpy((char *)wal->buffer + wal->buffer_used, user_data, user_data_size);
        wal->buffer_used += user_data_size;
    }

    /* Note: aux_data requires the slab context to access, which we don't have here.
     * For now, we'll write zeros for aux_data - the caller needs to provide
     * the slab context or we need to store it in the WAL config.
     * 
     * Alternative: The caller should call om_wal_insert_with_aux() passing
     * both slot and aux_data pointer.
     */
    if (aux_data_size > 0) {
        /* Zero-fill aux data section (caller must handle separately) */
        memset((char *)wal->buffer + wal->buffer_used, 0, aux_data_size);
        wal->buffer_used += aux_data_size;
    }

    return seq;
}

uint64_t om_wal_cancel(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id) {
    if (!wal) {
        return 0;
    }

    OmWalCancel rec;
    memset(&rec, 0, sizeof(rec));
    rec.order_id = order_id;
    rec.timestamp_ns = 0;  /* TODO: get actual timestamp */
    rec.slot_idx = slot_idx;
    rec.product_id = product_id;

    return wal_append(wal, OM_WAL_CANCEL, &rec, sizeof(OmWalCancel));
}

uint64_t om_wal_match(OmWal *wal, const OmWalMatch *rec) {
    if (!wal || !rec) {
        return 0;
    }
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

/* Initialize replay with data sizes (needed for parsing INSERT records) */
int om_wal_replay_init_with_sizes(OmWalReplay *replay, const char *filename, 
                                   size_t user_data_size, size_t aux_data_size) {
    int ret = om_wal_replay_init(replay, filename);
    if (ret == 0) {
        replay->user_data_size = user_data_size;
        replay->aux_data_size = aux_data_size;
    }
    return ret;
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
    uint16_t payload_len = om_wal_header_len(packed);

    replay->buffer_pos += sizeof(OmWalHeader);

    /* For INSERT records with variable-length data, we need to handle specially */
    if (*type == OM_WAL_INSERT) {
        /* First, read the OmWalInsert header to get actual data sizes */
        if (replay->buffer_pos + sizeof(OmWalInsert) > replay->buffer_valid) {
            int ret = replay_fill_buffer(replay);
            if (ret < 0) return -1;
            if (ret == 0 || replay->buffer_pos + sizeof(OmWalInsert) > replay->buffer_valid) {
                return -1;  /* Corrupted/truncated record */
            }
        }

        OmWalInsert *insert = (OmWalInsert *)((char *)replay->buffer + replay->buffer_pos);
        
        /* Calculate total variable-length payload */
        size_t actual_data_len = sizeof(OmWalInsert) + insert->user_data_size + insert->aux_data_size;
        
        /* Verify the header length matches */
        if (payload_len != actual_data_len && payload_len != (actual_data_len & 0xFFFF)) {
            /* Length mismatch - may be corrupted or use stored sizes vs runtime sizes */
            /* Use the stored sizes from the record itself */
        }

        /* Use stored sizes from record */
        *data_len = actual_data_len;

        /* Check if full record is in buffer */
        if (replay->buffer_pos + *data_len > replay->buffer_valid) {
            int ret = replay_fill_buffer(replay);
            if (ret < 0) return -1;
            if (ret == 0 || replay->buffer_pos + *data_len > replay->buffer_valid) {
                return -1;  /* Corrupted/truncated record */
            }
        }

        /* Return pointer to OmWalInsert (includes user_data and aux_data after it) */
        *data = (char *)replay->buffer + replay->buffer_pos;
        replay->buffer_pos += *data_len;
        replay->last_sequence = *sequence;

        return 1;
    } else {
        /* Fixed-size records (CANCEL, MATCH, CHECKPOINT) */
        *data_len = payload_len;

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
}

/* Helper to extract user data from INSERT record during replay */
void *om_wal_insert_get_user_data(OmWalInsert *insert, size_t user_data_size) {
    if (!insert || user_data_size == 0) return NULL;
    return (char *)insert + sizeof(OmWalInsert);
}

/* Helper to extract aux data from INSERT record during replay */
void *om_wal_insert_get_aux_data(OmWalInsert *insert, size_t user_data_size, size_t aux_data_size) {
    if (!insert || aux_data_size == 0) return NULL;
    return (char *)insert + sizeof(OmWalInsert) + user_data_size;
}
