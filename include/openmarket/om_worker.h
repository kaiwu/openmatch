#ifndef OM_WORKER_H
#define OM_WORKER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdint.h>
#include <pthread.h>

/**
 * @file om_worker.h
 * @brief Low-level worker primitives for OpenMarket
 */

typedef struct OmMarketRingSlot {
    _Atomic uint64_t seq;
    void *ptr;
} OmMarketRingSlot;

typedef struct OmMarketRing {
    OmMarketRingSlot *slots;
    size_t capacity;
    size_t mask;
    uint32_t consumer_count;
    size_t notify_batch;
    _Atomic uint64_t head;
    _Atomic uint64_t min_tail;
    _Atomic uint64_t *consumer_tails;
    pthread_mutex_t wait_mutex;
    pthread_cond_t wait_cond;
} OmMarketRing;

typedef struct OmMarketRingConfig {
    size_t capacity;        /**< Must be power-of-two (e.g., 2048, 4096) */
    uint32_t consumer_count;
    size_t notify_batch;    /**< Notify waiters every N enqueues (0 = no notify) */
} OmMarketRingConfig;

/**
 * Initialize a single-producer, multi-consumer ring for WAL record pointers.
 * @param ring Ring instance
 * @param config Ring configuration
 * @return 0 on success, negative on error
 */
int om_market_ring_init(OmMarketRing *ring, const OmMarketRingConfig *config);

/**
 * Destroy ring resources.
 * @param ring Ring instance
 */
void om_market_ring_destroy(OmMarketRing *ring);

/**
 * Register a consumer index for use with dequeue calls.
 * @param ring Ring instance
 * @param consumer_index Index in [0, consumer_count)
 * @return 0 on success, negative on error
 */
int om_market_ring_register_consumer(OmMarketRing *ring, uint32_t consumer_index);

/**
 * Enqueue a WAL record pointer. Blocks (spins) until space is available.
 * @param ring Ring instance
 * @param ptr Pointer to WAL record
 * @return 0 on success, negative on error
 */
int om_market_ring_enqueue(OmMarketRing *ring, void *ptr);

/**
 * Try to dequeue a WAL record pointer for a specific consumer.
 * @param ring Ring instance
 * @param consumer_index Consumer index
 * @param out_ptr Output pointer location
 * @return 1 if dequeued, 0 if empty, negative on error
 */
int om_market_ring_dequeue(OmMarketRing *ring, uint32_t consumer_index, void **out_ptr);

/**
 * Dequeue up to max_count WAL record pointers for a consumer.
 * @param ring Ring instance
 * @param consumer_index Consumer index
 * @param out_ptrs Output array for pointers
 * @param max_count Maximum number of pointers to dequeue
 * @return Number of pointers dequeued, or negative on error
 */
int om_market_ring_dequeue_batch(OmMarketRing *ring,
                                uint32_t consumer_index,
                                void **out_ptrs,
                                size_t max_count);

/**
 * Wait until at least min_batch records are available for a consumer.
 * @param ring Ring instance
 * @param consumer_index Consumer index
 * @param min_batch Minimum number of available records to wake
 * @return 0 on success, negative on error
 */
int om_market_ring_wait(OmMarketRing *ring, uint32_t consumer_index, size_t min_batch);

#endif
