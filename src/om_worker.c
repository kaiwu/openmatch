#include "openmarket/om_worker.h"
#include <stdlib.h>
#include <string.h>

static bool om_market_is_power_of_two(size_t value) {
    return value != 0 && (value & (value - 1U)) == 0U;
}

int om_market_ring_init(OmMarketRing *ring, const OmMarketRingConfig *config) {
    if (!ring || !config || config->capacity == 0 || config->consumer_count == 0) {
        return -1;
    }
    if (!om_market_is_power_of_two(config->capacity)) {
        return -2;
    }
    memset(ring, 0, sizeof(*ring));
    ring->capacity = config->capacity;
    ring->mask = config->capacity - 1U;
    ring->consumer_count = config->consumer_count;
    ring->notify_batch = config->notify_batch;

    ring->slots = calloc(config->capacity, sizeof(*ring->slots));
    if (!ring->slots) {
        return -3;
    }
    ring->consumer_tails = calloc(config->consumer_count, sizeof(*ring->consumer_tails));
    if (!ring->consumer_tails) {
        free(ring->slots);
        ring->slots = NULL;
        return -4;
    }

    for (size_t i = 0; i < config->capacity; i++) {
        atomic_init(&ring->slots[i].seq, (uint64_t)i);
        ring->slots[i].ptr = NULL;
    }
    for (uint32_t i = 0; i < config->consumer_count; i++) {
        atomic_init(&ring->consumer_tails[i], 0U);
    }
    atomic_init(&ring->head, 0U);
    atomic_init(&ring->min_tail, 0U);
    if (pthread_mutex_init(&ring->wait_mutex, NULL) != 0) {
        free(ring->consumer_tails);
        free(ring->slots);
        ring->consumer_tails = NULL;
        ring->slots = NULL;
        return -5;
    }
    if (pthread_cond_init(&ring->wait_cond, NULL) != 0) {
        pthread_mutex_destroy(&ring->wait_mutex);
        free(ring->consumer_tails);
        free(ring->slots);
        ring->consumer_tails = NULL;
        ring->slots = NULL;
        return -6;
    }
    return 0;
}

void om_market_ring_destroy(OmMarketRing *ring) {
    if (!ring) {
        return;
    }
    pthread_cond_destroy(&ring->wait_cond);
    pthread_mutex_destroy(&ring->wait_mutex);
    free(ring->slots);
    free(ring->consumer_tails);
    memset(ring, 0, sizeof(*ring));
}

int om_market_ring_register_consumer(OmMarketRing *ring, uint32_t consumer_index) {
    if (!ring || consumer_index >= ring->consumer_count) {
        return -1;
    }
    atomic_store_explicit(&ring->consumer_tails[consumer_index], 0U, memory_order_release);
    return 0;
}

static uint64_t om_market_ring_min_tail(const OmMarketRing *ring) {
    uint64_t min_tail = UINT64_MAX;
    for (uint32_t i = 0; i < ring->consumer_count; i++) {
        uint64_t value = atomic_load_explicit(&ring->consumer_tails[i], memory_order_acquire);
        if (value < min_tail) {
            min_tail = value;
        }
    }
    return min_tail == UINT64_MAX ? 0U : min_tail;
}

int om_market_ring_enqueue(OmMarketRing *ring, void *ptr) {
    if (!ring || !ptr) {
        return -1;
    }

    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    while (1) {
        uint64_t min_tail = atomic_load_explicit(&ring->min_tail, memory_order_acquire);
        if ((head - min_tail) < ring->capacity) {
            break;
        }
        min_tail = om_market_ring_min_tail(ring);
        atomic_store_explicit(&ring->min_tail, min_tail, memory_order_release);
    }

    size_t idx = (size_t)(head & ring->mask);
    while (atomic_load_explicit(&ring->slots[idx].seq, memory_order_acquire) != head) {
    }

    ring->slots[idx].ptr = ptr;
    atomic_store_explicit(&ring->slots[idx].seq, head + 1U, memory_order_release);
    atomic_store_explicit(&ring->head, head + 1U, memory_order_release);

    if (ring->notify_batch > 0 && ((head + 1U) % ring->notify_batch) == 0U) {
        pthread_mutex_lock(&ring->wait_mutex);
        pthread_cond_broadcast(&ring->wait_cond);
        pthread_mutex_unlock(&ring->wait_mutex);
    }
    return 0;
}

int om_market_ring_dequeue(OmMarketRing *ring, uint32_t consumer_index, void **out_ptr) {
    if (!ring || !out_ptr || consumer_index >= ring->consumer_count) {
        return -1;
    }

    uint64_t tail = atomic_load_explicit(&ring->consumer_tails[consumer_index],
                                         memory_order_relaxed);
    size_t idx = (size_t)(tail & ring->mask);
    if (atomic_load_explicit(&ring->slots[idx].seq, memory_order_acquire) != tail + 1U) {
        return 0;
    }

    void *ptr = ring->slots[idx].ptr;
    atomic_store_explicit(&ring->consumer_tails[consumer_index], tail + 1U,
                          memory_order_release);
    *out_ptr = ptr;
    return 1;
}

int om_market_ring_dequeue_batch(OmMarketRing *ring,
                                uint32_t consumer_index,
                                void **out_ptrs,
                                size_t max_count) {
    if (!ring || !out_ptrs || max_count == 0 || consumer_index >= ring->consumer_count) {
        return -1;
    }

    uint64_t tail = atomic_load_explicit(&ring->consumer_tails[consumer_index],
                                         memory_order_relaxed);
    size_t count = 0;

    while (count < max_count) {
        size_t idx = (size_t)(tail & ring->mask);
        if (atomic_load_explicit(&ring->slots[idx].seq, memory_order_acquire) != tail + 1U) {
            break;
        }
        out_ptrs[count] = ring->slots[idx].ptr;
        tail++;
        count++;
    }

    if (count > 0) {
        atomic_store_explicit(&ring->consumer_tails[consumer_index], tail, memory_order_release);
    }
    return (int)count;
}

int om_market_ring_wait(OmMarketRing *ring, uint32_t consumer_index, size_t min_batch) {
    if (!ring || consumer_index >= ring->consumer_count || min_batch == 0) {
        return -1;
    }

    pthread_mutex_lock(&ring->wait_mutex);
    while (1) {
        uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
        uint64_t tail = atomic_load_explicit(&ring->consumer_tails[consumer_index],
                                             memory_order_acquire);
        if ((head - tail) >= min_batch) {
            break;
        }
        pthread_cond_wait(&ring->wait_cond, &ring->wait_mutex);
    }
    pthread_mutex_unlock(&ring->wait_mutex);
    return 0;
}
