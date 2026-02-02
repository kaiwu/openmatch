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
#define WAL_CRC32_SIZE 4

/*
 * Get current timestamp in nanoseconds using CLOCK_MONOTONIC.
 * CLOCK_MONOTONIC is used instead of CLOCK_REALTIME because:
 * - It is not affected by NTP adjustments or manual clock changes
 * - It provides consistent, monotonically increasing values
 * - It's ideal for ordering events within a single system run
 * Note: For cross-system timestamp correlation, consider CLOCK_REALTIME.
 */
static inline uint64_t wal_get_timestamp_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
        return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    }
    return 0;
}

/* CRC32 lookup table (IEEE 802.3 polynomial: 0xEDB88320) */
static uint32_t crc32_table[256];
static bool crc32_table_initialized = false;

static void crc32_init_table(void) {
    if (crc32_table_initialized) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
        }
        crc32_table[i] = crc;
    }
    crc32_table_initialized = true;
}

static uint32_t crc32_compute(const void *data, size_t len) {
    crc32_init_table();
    const uint8_t *buf = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = crc32_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

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

static uint64_t wal_scan_for_last_sequence(const char *filename, const OmWalConfig *config) {
    int fd = open(filename, O_RDONLY);
    if (fd < 0) return 0;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size == 0) {
        close(fd);
        return 0;
    }

    size_t buf_size = 64 * 1024;
    uint8_t *buf = malloc(buf_size);
    if (!buf) {
        close(fd);
        return 0;
    }

    uint64_t last_seq = 0;
    size_t valid = 0;
    size_t pos = 0;
    bool eof = false;
    size_t crc_size = config->enable_crc32 ? WAL_CRC32_SIZE : 0;

    while (!eof) {
        if (pos + WAL_HEADER_SIZE > valid) {
            size_t remaining = valid - pos;
            if (remaining > 0) {
                memmove(buf, buf + pos, remaining);
            }
            ssize_t n = read(fd, buf + remaining, buf_size - remaining);
            if (n <= 0) {
                eof = true;
                break;
            }
            valid = remaining + (size_t)n;
            pos = 0;
            if (pos + WAL_HEADER_SIZE > valid) {
                break;
            }
        }

        OmWalHeader *hdr = (OmWalHeader *)(buf + pos);
        uint64_t packed = hdr->seq_type_len;
        uint64_t seq = om_wal_header_seq(packed);
        uint8_t type = om_wal_header_type(packed);
        uint16_t payload_len = om_wal_header_len(packed);

        if (type < OM_WAL_INSERT || type > OM_WAL_CHECKPOINT) {
            break;
        }

        size_t record_size = WAL_HEADER_SIZE + payload_len + crc_size;
        if (pos + record_size > valid) {
            size_t remaining = valid - pos;
            if (remaining > 0) {
                memmove(buf, buf + pos, remaining);
            }
            ssize_t n = read(fd, buf + remaining, buf_size - remaining);
            if (n <= 0) {
                eof = true;
                break;
            }
            valid = remaining + (size_t)n;
            pos = 0;
            if (pos + record_size > valid) {
                break;
            }
        }

        last_seq = seq;
        pos += record_size;
    }

    free(buf);
    close(fd);
    return last_seq;
}

int om_wal_init(OmWal *wal, const OmWalConfig *config) {
    if (!wal || !config || !config->filename) {
        return -1;
    }

    memset(wal, 0, sizeof(OmWal));
    wal->config = *config;
    wal->slab = NULL;

    if (wal->config.buffer_size == 0) {
        wal->config.buffer_size = 1024 * 1024;
    }
    wal->config.buffer_size = (wal->config.buffer_size + WAL_ALIGN - 1) & ~WAL_ALIGN_MASK;

    wal->buffer_unaligned = malloc(wal->config.buffer_size + WAL_ALIGN);
    if (!wal->buffer_unaligned) {
        return -1;
    }
    wal->buffer = align_up(wal->buffer_unaligned, WAL_ALIGN);
    wal->buffer_size = wal->config.buffer_size;
    wal->buffer_used = 0;

    int flags = O_WRONLY | O_CREAT | O_APPEND;
    if (wal->config.use_direct_io) {
        flags |= O_DIRECT;
    }

    wal->fd = open(config->filename, flags, 0644);
    if (wal->fd < 0) {
        free(wal->buffer_unaligned);
        return -1;
    }

    struct stat st;
    if (fstat(wal->fd, &st) == 0) {
        wal->file_offset = st.st_size;
        if (st.st_size > 0) {
            uint64_t last_seq = wal_scan_for_last_sequence(config->filename, config);
            wal->sequence = (last_seq > 0) ? last_seq + 1 : 1;
        } else {
            wal->sequence = 1;
        }
    } else {
        wal->sequence = 1;
    }

    return 0;
}

void om_wal_set_slab(OmWal *wal, struct OmDualSlab *slab) {
    if (wal) {
        wal->slab = slab;
    }
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

static uint64_t wal_append(OmWal *wal, OmWalType type, const void *data, size_t data_size) {
    size_t crc_size = wal->config.enable_crc32 ? WAL_CRC32_SIZE : 0;
    size_t total_size = WAL_HEADER_SIZE + data_size + crc_size;
    
    if (wal->buffer_used + total_size > wal->buffer_size) {
        if (om_wal_flush(wal) != 0) {
            return 0;
        }
    }

    uint64_t seq = wal->sequence++;
    char *buf = (char *)wal->buffer + wal->buffer_used;

    uint64_t header = om_wal_pack_header(seq, type, (uint16_t)data_size);
    memcpy(buf, &header, WAL_HEADER_SIZE);
    wal->buffer_used += WAL_HEADER_SIZE;

    memcpy((char *)wal->buffer + wal->buffer_used, data, data_size);
    wal->buffer_used += data_size;

    if (wal->config.enable_crc32) {
        uint32_t crc = crc32_compute(buf, WAL_HEADER_SIZE + data_size);
        memcpy((char *)wal->buffer + wal->buffer_used, &crc, WAL_CRC32_SIZE);
        wal->buffer_used += WAL_CRC32_SIZE;
    }

    return seq;
}

uint64_t om_wal_insert(OmWal *wal, struct OmSlabSlot *slot, uint16_t product_id) {
    if (!wal || !slot) {
        return 0;
    }

    size_t user_data_size = wal->config.user_data_size;
    size_t aux_data_size = wal->config.aux_data_size;
    size_t crc_size = wal->config.enable_crc32 ? WAL_CRC32_SIZE : 0;
    size_t data_size = sizeof(OmWalInsert) + user_data_size + aux_data_size;
    size_t total_size = WAL_HEADER_SIZE + data_size + crc_size;
    total_size = (total_size + 7) & ~7;

    if (wal->buffer_used + total_size > wal->buffer_size) {
        if (om_wal_flush(wal) != 0) {
            return 0;
        }
    }

    uint64_t seq = wal->sequence++;
    char *record_start = (char *)wal->buffer + wal->buffer_used;

    uint64_t header = om_wal_pack_header(seq, OM_WAL_INSERT, (uint16_t)data_size);
    memcpy(record_start, &header, WAL_HEADER_SIZE);
    wal->buffer_used += WAL_HEADER_SIZE;

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
    insert.timestamp_ns = wal_get_timestamp_ns();

    memcpy((char *)wal->buffer + wal->buffer_used, &insert, sizeof(OmWalInsert));
    wal->buffer_used += sizeof(OmWalInsert);

    if (user_data_size > 0) {
        void *user_data = om_slot_get_data(slot);
        memcpy((char *)wal->buffer + wal->buffer_used, user_data, user_data_size);
        wal->buffer_used += user_data_size;
    }

    if (aux_data_size > 0) {
        if (wal->slab) {
            void *aux_data = om_slot_get_aux_data(wal->slab, slot);
            memcpy((char *)wal->buffer + wal->buffer_used, aux_data, aux_data_size);
        } else {
            memset((char *)wal->buffer + wal->buffer_used, 0, aux_data_size);
        }
        wal->buffer_used += aux_data_size;
    }

    if (wal->config.enable_crc32) {
        uint32_t crc = crc32_compute(record_start, WAL_HEADER_SIZE + data_size);
        memcpy((char *)wal->buffer + wal->buffer_used, &crc, WAL_CRC32_SIZE);
        wal->buffer_used += WAL_CRC32_SIZE;
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
    rec.timestamp_ns = wal_get_timestamp_ns();
    rec.slot_idx = slot_idx;
    rec.product_id = product_id;

    return wal_append(wal, OM_WAL_CANCEL, &rec, sizeof(OmWalCancel));
}

uint64_t om_wal_deactivate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id) {
    if (!wal) {
        return 0;
    }

    OmWalDeactivate rec;
    memset(&rec, 0, sizeof(rec));
    rec.order_id = order_id;
    rec.timestamp_ns = wal_get_timestamp_ns();
    rec.slot_idx = slot_idx;
    rec.product_id = product_id;

    return wal_append(wal, OM_WAL_DEACTIVATE, &rec, sizeof(OmWalDeactivate));
}

uint64_t om_wal_activate(OmWal *wal, uint32_t order_id, uint32_t slot_idx, uint16_t product_id) {
    if (!wal) {
        return 0;
    }

    OmWalActivate rec;
    memset(&rec, 0, sizeof(rec));
    rec.order_id = order_id;
    rec.timestamp_ns = wal_get_timestamp_ns();
    rec.slot_idx = slot_idx;
    rec.product_id = product_id;

    return wal_append(wal, OM_WAL_ACTIVATE, &rec, sizeof(OmWalActivate));
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

int om_wal_replay_init_with_sizes(OmWalReplay *replay, const char *filename, 
                                   size_t user_data_size, size_t aux_data_size) {
    int ret = om_wal_replay_init(replay, filename);
    if (ret == 0) {
        replay->user_data_size = user_data_size;
        replay->aux_data_size = aux_data_size;
    }
    return ret;
}

int om_wal_replay_init_with_config(OmWalReplay *replay, const char *filename,
                                    const OmWalConfig *config) {
    if (!config) {
        return -1;
    }
    int ret = om_wal_replay_init(replay, filename);
    if (ret == 0) {
        replay->user_data_size = config->user_data_size;
        replay->aux_data_size = config->aux_data_size;
        replay->enable_crc32 = config->enable_crc32;
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

    size_t crc_size = replay->enable_crc32 ? WAL_CRC32_SIZE : 0;

    if (replay->buffer_pos + sizeof(OmWalHeader) > replay->buffer_valid) {
        int ret = replay_fill_buffer(replay);
        if (ret < 0) return -1;
        if (ret == 0) return 0;
        
        if (replay->buffer_pos + sizeof(OmWalHeader) > replay->buffer_valid) {
            return 0;
        }
    }

    char *record_start = (char *)replay->buffer + replay->buffer_pos;
    
    /* Use memcpy to avoid unaligned access (UBSan) */
    OmWalHeader header_local;
    memcpy(&header_local, record_start, sizeof(OmWalHeader));
    uint64_t packed = header_local.seq_type_len;
    *sequence = om_wal_header_seq(packed);
    uint8_t type_byte = om_wal_header_type(packed);
    uint16_t payload_len = om_wal_header_len(packed);
    
    /* Treat invalid type as EOF (handles zero padding at file end) */
    if (type_byte < OM_WAL_INSERT || type_byte > OM_WAL_CHECKPOINT) {
        return 0;  /* EOF - not a valid record */
    }
    *type = (OmWalType)type_byte;

    replay->buffer_pos += sizeof(OmWalHeader);

    if (*type == OM_WAL_INSERT) {
        if (replay->buffer_pos + sizeof(OmWalInsert) > replay->buffer_valid) {
            int ret = replay_fill_buffer(replay);
            if (ret < 0) return -1;
            if (ret == 0 || replay->buffer_pos + sizeof(OmWalInsert) > replay->buffer_valid) {
                return -1;
            }
            record_start = (char *)replay->buffer + replay->buffer_pos - sizeof(OmWalHeader);
        }

        /* Use memcpy to read OmWalInsert header to avoid unaligned access */
        OmWalInsert insert_local;
        memcpy(&insert_local, (char *)replay->buffer + replay->buffer_pos, sizeof(OmWalInsert));
        size_t actual_data_len = sizeof(OmWalInsert) + insert_local.user_data_size + insert_local.aux_data_size;
        *data_len = actual_data_len;

        size_t needed = *data_len + crc_size;
        if (replay->buffer_pos + needed > replay->buffer_valid) {
            int ret = replay_fill_buffer(replay);
            if (ret < 0) return -1;
            if (ret == 0 || replay->buffer_pos + needed > replay->buffer_valid) {
                return -1;
            }
            record_start = (char *)replay->buffer + replay->buffer_pos - sizeof(OmWalHeader);
        }

        if (replay->enable_crc32) {
            uint32_t stored_crc;
            memcpy(&stored_crc, (char *)replay->buffer + replay->buffer_pos + *data_len, WAL_CRC32_SIZE);
            uint32_t computed_crc = crc32_compute(record_start, sizeof(OmWalHeader) + *data_len);
            if (stored_crc != computed_crc) {
                return -2;
            }
        }

        *data = (char *)replay->buffer + replay->buffer_pos;
        replay->buffer_pos += *data_len + crc_size;
        replay->last_sequence = *sequence;

        return 1;
    } else {
        *data_len = payload_len;

        size_t needed = *data_len + crc_size;
        if (replay->buffer_pos + needed > replay->buffer_valid) {
            int ret = replay_fill_buffer(replay);
            if (ret < 0) return -1;
            if (ret == 0 || replay->buffer_pos + needed > replay->buffer_valid) {
                return -1;
            }
            record_start = (char *)replay->buffer + replay->buffer_pos - sizeof(OmWalHeader);
        }

        if (replay->enable_crc32) {
            uint32_t stored_crc;
            memcpy(&stored_crc, (char *)replay->buffer + replay->buffer_pos + *data_len, WAL_CRC32_SIZE);
            uint32_t computed_crc = crc32_compute(record_start, sizeof(OmWalHeader) + *data_len);
            if (stored_crc != computed_crc) {
                return -2;
            }
        }

        *data = (char *)replay->buffer + replay->buffer_pos;
        replay->buffer_pos += *data_len + crc_size;
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
