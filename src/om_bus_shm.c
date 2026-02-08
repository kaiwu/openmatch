#include "ombus/om_bus.h"

#include <fcntl.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

/* ============================================================================
 * Internal helpers
 * ============================================================================ */

static inline void _om_bus_cpu_relax(void) {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#endif
}

static inline bool _om_bus_is_power_of_two(uint32_t v) {
    return v != 0 && (v & (v - 1U)) == 0U;
}

/* CRC32 (IEEE 802.3 polynomial: 0xEDB88320) */
static uint32_t _om_bus_crc32_table[256];
static bool _om_bus_crc32_ready = false;

static void _om_bus_crc32_init(void) {
    if (_om_bus_crc32_ready) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c >> 1) ^ ((c & 1) ? 0xEDB88320U : 0U);
        }
        _om_bus_crc32_table[i] = c;
    }
    _om_bus_crc32_ready = true;
}

static uint32_t _om_bus_crc32(const void *data, size_t len) {
    _om_bus_crc32_init();
    const uint8_t *buf = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = _om_bus_crc32_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

/* Compute total SHM file size */
static size_t _om_bus_shm_size(uint32_t capacity, uint32_t slot_size,
                                uint32_t max_consumers) {
    return OM_BUS_HEADER_PAGE
         + (size_t)max_consumers * OM_BUS_CONSUMER_ALIGN
         + (size_t)capacity * slot_size;
}

/* Pointer to consumer tail array (after header page) */
static inline OmBusConsumerTail *_om_bus_consumer_tails(void *base) {
    return (OmBusConsumerTail *)((char *)base + OM_BUS_HEADER_PAGE);
}

/* Pointer to slot i (after header page + consumer tails) */
static inline void *_om_bus_slot(void *base, uint32_t max_consumers,
                                  uint32_t slot_size, uint64_t idx) {
    char *slots_base = (char *)base + OM_BUS_HEADER_PAGE
                     + (size_t)max_consumers * OM_BUS_CONSUMER_ALIGN;
    return slots_base + idx * slot_size;
}

/* Scan consumer tails and return the minimum */
static uint64_t _om_bus_min_tail(OmBusConsumerTail *tails, uint32_t count) {
    uint64_t min_val = UINT64_MAX;
    for (uint32_t i = 0; i < count; i++) {
        uint64_t t = atomic_load_explicit(&tails[i].tail, memory_order_acquire);
        if (t < min_val) min_val = t;
    }
    return min_val == UINT64_MAX ? 0U : min_val;
}

/* ============================================================================
 * OmBusStream — producer
 * ============================================================================ */

struct OmBusStream {
    void *map;                  /* mmap base pointer */
    size_t map_size;            /* mmap region length */
    OmBusShmHeader *hdr;        /* == map */
    OmBusConsumerTail *tails;   /* consumer tail array */
    uint32_t slot_size;
    uint32_t capacity;
    uint32_t mask;
    uint32_t max_consumers;
    uint32_t flags;
    char shm_name[64];         /* for shm_unlink on destroy */
};

int om_bus_stream_create(OmBusStream **out, const OmBusStreamConfig *config) {
    if (!out || !config || !config->stream_name) {
        return OM_ERR_BUS_INIT;
    }

    uint32_t capacity = config->capacity ? config->capacity : OM_BUS_DEFAULT_CAPACITY;
    uint32_t slot_size = config->slot_size ? config->slot_size : OM_BUS_DEFAULT_SLOT_SIZE;
    uint32_t max_consumers = config->max_consumers ? config->max_consumers : OM_BUS_DEFAULT_MAX_CONSUMERS;

    if (!_om_bus_is_power_of_two(capacity)) {
        return OM_ERR_BUS_NOT_POW2;
    }
    if (slot_size < OM_BUS_SLOT_HEADER_SIZE + 1) {
        return OM_ERR_BUS_INIT;
    }

    size_t total = _om_bus_shm_size(capacity, slot_size, max_consumers);

    /* Create SHM object */
    int fd = shm_open(config->stream_name, O_CREAT | O_RDWR | O_TRUNC, 0600);
    if (fd < 0) {
        return OM_ERR_BUS_SHM_CREATE;
    }
    if (ftruncate(fd, (off_t)total) != 0) {
        close(fd);
        shm_unlink(config->stream_name);
        return OM_ERR_BUS_SHM_CREATE;
    }

    void *map = mmap(NULL, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        shm_unlink(config->stream_name);
        return OM_ERR_BUS_SHM_MAP;
    }

    /* Zero-fill (mmap of ftruncate'd file is already zeroed, but be explicit) */
    memset(map, 0, total);

    /* Initialize header */
    OmBusShmHeader *hdr = (OmBusShmHeader *)map;
    hdr->magic = OM_BUS_SHM_MAGIC;
    hdr->version = OM_BUS_SHM_VERSION;
    hdr->slot_size = slot_size;
    hdr->capacity = capacity;
    hdr->max_consumers = max_consumers;
    hdr->flags = config->flags;
    atomic_init(&hdr->head, 0U);
    atomic_init(&hdr->min_tail, 0U);
    strncpy(hdr->stream_name, config->stream_name, sizeof(hdr->stream_name) - 1);
    hdr->stream_name[sizeof(hdr->stream_name) - 1] = '\0';

    /* Initialize consumer tails */
    OmBusConsumerTail *tails = _om_bus_consumer_tails(map);
    for (uint32_t i = 0; i < max_consumers; i++) {
        atomic_init(&tails[i].tail, 0U);
        atomic_init(&tails[i].wal_seq, 0U);
    }

    /* Initialize slot sequences */
    for (uint32_t i = 0; i < capacity; i++) {
        OmBusSlotHeader *slot = (OmBusSlotHeader *)_om_bus_slot(
            map, max_consumers, slot_size, i);
        atomic_init(&slot->seq, (uint64_t)i);
    }

    /* Allocate stream handle */
    OmBusStream *s = calloc(1, sizeof(*s));
    if (!s) {
        munmap(map, total);
        shm_unlink(config->stream_name);
        return OM_ERR_BUS_INIT;
    }
    s->map = map;
    s->map_size = total;
    s->hdr = hdr;
    s->tails = tails;
    s->slot_size = slot_size;
    s->capacity = capacity;
    s->mask = capacity - 1U;
    s->max_consumers = max_consumers;
    s->flags = config->flags;
    strncpy(s->shm_name, config->stream_name, sizeof(s->shm_name) - 1);
    s->shm_name[sizeof(s->shm_name) - 1] = '\0';

    *out = s;
    return 0;
}

int om_bus_stream_publish(OmBusStream *stream, uint64_t wal_seq,
                          uint8_t wal_type, const void *payload, uint16_t len) {
    if (!stream) return OM_ERR_BUS_INIT;
    if (len > stream->slot_size - OM_BUS_SLOT_HEADER_SIZE) {
        return OM_ERR_BUS_RECORD_TOO_LARGE;
    }

    uint64_t head = atomic_load_explicit(&stream->hdr->head, memory_order_relaxed);

    /* Backpressure: spin while ring is full */
    uint32_t pressure_spins = 0;
    while (1) {
        uint64_t mt = atomic_load_explicit(&stream->hdr->min_tail, memory_order_acquire);
        if ((head - mt) < stream->capacity) break;
        if ((pressure_spins & 31U) == 0U) {
            mt = _om_bus_min_tail(stream->tails, stream->max_consumers);
            atomic_store_explicit(&stream->hdr->min_tail, mt, memory_order_release);
        } else {
            _om_bus_cpu_relax();
        }
        pressure_spins++;
        if ((pressure_spins & 1023U) == 0U) sched_yield();
    }

    uint64_t idx = head & stream->mask;
    OmBusSlotHeader *slot = (OmBusSlotHeader *)_om_bus_slot(
        stream->map, stream->max_consumers, stream->slot_size, idx);

    /* Backpressure above guarantees head - min_tail < capacity, so this
     * slot has been consumed by all consumers and is safe to overwrite.
     * No slot-level seq spin needed (single producer). */

    /* Copy payload into slot */
    char *payload_dst = (char *)slot + OM_BUS_SLOT_HEADER_SIZE;
    if (payload && len > 0) {
        memcpy(payload_dst, payload, len);
    }

    /* Fill non-atomic header fields */
    slot->wal_seq = wal_seq;
    slot->wal_type = wal_type;
    slot->reserved = 0;
    slot->payload_len = len;
    slot->crc32 = (stream->flags & OM_BUS_FLAG_CRC) ? _om_bus_crc32(payload, len) : 0;

    /* Publish fence: make payload visible before seq update */
    atomic_store_explicit(&slot->seq, head + 1U, memory_order_release);

    /* Advance head */
    atomic_store_explicit(&stream->hdr->head, head + 1U, memory_order_release);

    return 0;
}

void om_bus_stream_destroy(OmBusStream *stream) {
    if (!stream) return;
    if (stream->map && stream->map != MAP_FAILED) {
        munmap(stream->map, stream->map_size);
    }
    shm_unlink(stream->shm_name);
    free(stream);
}

/* ============================================================================
 * OmBusEndpoint — consumer
 * ============================================================================ */

struct OmBusEndpoint {
    void *map;                  /* mmap base pointer */
    size_t map_size;            /* mmap region length */
    OmBusShmHeader *hdr;
    OmBusConsumerTail *tails;
    uint32_t consumer_index;
    uint32_t slot_size;
    uint32_t capacity;
    uint32_t mask;
    uint32_t max_consumers;
    uint32_t flags;
    bool zero_copy;
    uint64_t expected_wal_seq;  /* For gap detection */
    void *copy_buf;             /* Copy buffer (when !zero_copy) */
};

int om_bus_endpoint_open(OmBusEndpoint **out, const OmBusEndpointConfig *config) {
    if (!out || !config || !config->stream_name) {
        return OM_ERR_BUS_INIT;
    }

    int fd = shm_open(config->stream_name, O_RDWR, 0);
    if (fd < 0) {
        return OM_ERR_BUS_SHM_OPEN;
    }

    /* Read header page first to learn layout */
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < (off_t)OM_BUS_HEADER_PAGE) {
        close(fd);
        return OM_ERR_BUS_SHM_OPEN;
    }

    size_t total = (size_t)st.st_size;
    void *map = mmap(NULL, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) {
        return OM_ERR_BUS_SHM_MAP;
    }

    OmBusShmHeader *hdr = (OmBusShmHeader *)map;

    /* Validate magic & version */
    if (hdr->magic != OM_BUS_SHM_MAGIC) {
        munmap(map, total);
        return OM_ERR_BUS_MAGIC_MISMATCH;
    }
    if (hdr->version != OM_BUS_SHM_VERSION) {
        munmap(map, total);
        return OM_ERR_BUS_VERSION_MISMATCH;
    }
    if (config->consumer_index >= hdr->max_consumers) {
        munmap(map, total);
        return OM_ERR_BUS_CONSUMER_ID;
    }

    OmBusEndpoint *ep = calloc(1, sizeof(*ep));
    if (!ep) {
        munmap(map, total);
        return OM_ERR_BUS_INIT;
    }

    ep->map = map;
    ep->map_size = total;
    ep->hdr = hdr;
    ep->tails = _om_bus_consumer_tails(map);
    ep->consumer_index = config->consumer_index;
    ep->slot_size = hdr->slot_size;
    ep->capacity = hdr->capacity;
    ep->mask = hdr->capacity - 1U;
    ep->max_consumers = hdr->max_consumers;
    ep->flags = hdr->flags;
    ep->zero_copy = config->zero_copy;
    ep->expected_wal_seq = 0;

    if (!config->zero_copy) {
        ep->copy_buf = malloc(hdr->slot_size);
        if (!ep->copy_buf) {
            munmap(map, total);
            free(ep);
            return OM_ERR_BUS_INIT;
        }
    }

    /* Initialize consumer tail to current head (start from live position) */
    uint64_t cur_head = atomic_load_explicit(&hdr->head, memory_order_acquire);
    atomic_store_explicit(&ep->tails[config->consumer_index].tail,
                          cur_head, memory_order_release);
    atomic_store_explicit(&ep->tails[config->consumer_index].wal_seq,
                          0U, memory_order_release);

    *out = ep;
    return 0;
}

int om_bus_endpoint_poll(OmBusEndpoint *ep, OmBusRecord *rec) {
    if (!ep || !rec) return OM_ERR_BUS_INIT;

    uint64_t tail = atomic_load_explicit(
        &ep->tails[ep->consumer_index].tail, memory_order_relaxed);
    uint64_t idx = tail & ep->mask;

    OmBusSlotHeader *slot = (OmBusSlotHeader *)_om_bus_slot(
        ep->map, ep->max_consumers, ep->slot_size, idx);

    /* Check if slot is ready */
    if (atomic_load_explicit(&slot->seq, memory_order_acquire) != tail + 1U) {
        return 0; /* empty */
    }

    /* Read header fields */
    rec->wal_seq = slot->wal_seq;
    rec->wal_type = slot->wal_type;
    rec->payload_len = slot->payload_len;

    const void *payload_src = (const char *)slot + OM_BUS_SLOT_HEADER_SIZE;

    /* CRC check */
    if (ep->flags & OM_BUS_FLAG_CRC) {
        uint32_t computed = _om_bus_crc32(payload_src, slot->payload_len);
        if (computed != slot->crc32) {
            return OM_ERR_BUS_CRC_MISMATCH;
        }
    }

    /* Deliver payload */
    if (ep->zero_copy) {
        rec->payload = payload_src;
    } else {
        memcpy(ep->copy_buf, payload_src, slot->payload_len);
        rec->payload = ep->copy_buf;
    }

    /* Gap detection */
    int result = 1;
    if (ep->expected_wal_seq > 0 && rec->wal_seq != ep->expected_wal_seq) {
        if (rec->wal_seq > ep->expected_wal_seq) {
            result = OM_ERR_BUS_GAP_DETECTED;
        }
        /* wal_seq < expected: duplicate/reorder — skip detection, just advance */
    }
    ep->expected_wal_seq = rec->wal_seq + 1;

    /* Advance tail */
    uint64_t prev_tail = tail;
    uint64_t new_tail = tail + 1U;
    atomic_store_explicit(&ep->tails[ep->consumer_index].tail,
                          new_tail, memory_order_release);
    atomic_store_explicit(&ep->tails[ep->consumer_index].wal_seq,
                          rec->wal_seq, memory_order_release);

    /* Refresh min_tail if we were the minimum */
    uint64_t cached_min = atomic_load_explicit(&ep->hdr->min_tail, memory_order_acquire);
    if (prev_tail == cached_min || new_tail < cached_min) {
        uint64_t mt = _om_bus_min_tail(ep->tails, ep->max_consumers);
        atomic_store_explicit(&ep->hdr->min_tail, mt, memory_order_release);
    }

    return result;
}

int om_bus_endpoint_poll_batch(OmBusEndpoint *ep, OmBusRecord *recs,
                               size_t max_count) {
    if (!ep || !recs) return OM_ERR_BUS_INIT;
    if (max_count == 0) return 0;

    uint64_t tail = atomic_load_explicit(
        &ep->tails[ep->consumer_index].tail, memory_order_relaxed);
    size_t count = 0;

    while (count < max_count) {
        uint64_t idx = (tail + count) & ep->mask;

        OmBusSlotHeader *slot = (OmBusSlotHeader *)_om_bus_slot(
            ep->map, ep->max_consumers, ep->slot_size, idx);

        if (atomic_load_explicit(&slot->seq, memory_order_acquire) != tail + count + 1U) {
            break;
        }

        recs[count].wal_seq = slot->wal_seq;
        recs[count].wal_type = slot->wal_type;
        recs[count].payload_len = slot->payload_len;

        const void *payload_src = (const char *)slot + OM_BUS_SLOT_HEADER_SIZE;

        if (ep->flags & OM_BUS_FLAG_CRC) {
            uint32_t computed = _om_bus_crc32(payload_src, slot->payload_len);
            if (computed != slot->crc32) {
                break; /* stop batch on CRC error */
            }
        }

        if (ep->zero_copy) {
            recs[count].payload = payload_src;
        } else {
            /* For batch, each record needs its own copy area.
             * We only have one copy buffer, so batch forces zero_copy semantics
             * for the payload pointer (points into mmap). Caller must process
             * before the producer wraps. */
            recs[count].payload = payload_src;
        }

        count++;
    }

    if (count > 0) {
        uint64_t prev_tail = tail;
        uint64_t new_tail = tail + count;
        atomic_store_explicit(&ep->tails[ep->consumer_index].tail,
                              new_tail, memory_order_release);
        atomic_store_explicit(&ep->tails[ep->consumer_index].wal_seq,
                              recs[count - 1].wal_seq, memory_order_release);

        uint64_t cached_min = atomic_load_explicit(&ep->hdr->min_tail, memory_order_acquire);
        if (prev_tail == cached_min || new_tail < cached_min) {
            uint64_t mt = _om_bus_min_tail(ep->tails, ep->max_consumers);
            atomic_store_explicit(&ep->hdr->min_tail, mt, memory_order_release);
        }
    }

    return (int)count;
}

uint64_t om_bus_endpoint_wal_seq(const OmBusEndpoint *ep) {
    if (!ep) return 0;
    return atomic_load_explicit(&ep->tails[ep->consumer_index].wal_seq,
                                memory_order_acquire);
}

void om_bus_endpoint_close(OmBusEndpoint *ep) {
    if (!ep) return;
    free(ep->copy_buf);
    if (ep->map && ep->map != MAP_FAILED) {
        munmap(ep->map, ep->map_size);
    }
    free(ep);
}
