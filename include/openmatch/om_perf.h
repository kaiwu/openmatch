#ifndef OM_PERF_H
#define OM_PERF_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/*
 * OpenMatch Performance Configuration
 * 
 * All major performance-tunable parameters in one structure.
 * Use this to optimize for your specific workload:
 * - High-frequency trading: small buffers, frequent syncs
 * - Batch processing: large buffers, infrequent syncs
 * - Recovery-focused: enable all durability features
 */

typedef struct OmPerfConfig {
    /* =========================================================================
     * SLAB ALLOCATOR PERFORMANCE
     * ========================================================================= */
    
    /* Slot capacity - affects memory usage and max orders */
    uint32_t slab_total_slots;      /* Total slots in both slabs (default: 1M) */
    
    /* Data sizes - must match your order structure */
    size_t slab_user_data_size;     /* Secondary hot data size (default: 64) */
    size_t slab_aux_data_size;      /* Cold/aux data size (default: 256) */
    
    /* 
     * Memory preallocation strategy:
     * - true: Preallocate all memory at init (faster runtime, higher startup cost)
     * - false: Allocate on demand (lower startup cost, potential runtime stalls)
     */
    bool slab_preallocate;          /* Preallocate all slots at init (default: true) */
    
    /* =========================================================================
     * HASHMAP PERFORMANCE (Order lookups by ID)
     * ========================================================================= */
    
    /* 
     * Initial hashmap capacity - affects resize frequency
     * Should be ~1.5x expected max concurrent orders
     */
    uint32_t hashmap_initial_cap;   /* Initial hashmap capacity (default: 1M) */
    
    /* 
     * Load factor before resize - tradeoff between memory and performance
     * - Lower (0.5): Faster lookups, more memory
     * - Higher (0.9): Less memory, slower lookups when full
     */
    float hashmap_load_factor;      /* Resize threshold (default: 0.75) */
    
    /* =========================================================================
     * WAL (Write-Ahead Log) PERFORMANCE
     * ========================================================================= */
    
    /* Buffer sizes - larger = fewer syscalls, more data at risk on crash */
    size_t wal_buffer_size;         /* Write buffer size (default: 1MB) */
    size_t wal_read_buffer_size;    /* Replay read buffer (default: 1MB) */
    
    /* Sync strategy - durability vs performance tradeoff */
    uint32_t wal_sync_interval_ms;  /* Auto-fsync interval, 0=disabled (default: 10) */
    bool wal_sync_on_insert;        /* Fsync every insert (default: false) */
    bool wal_sync_on_cancel;        /* Fsync every cancel (default: false) */
    
    /* I/O strategy */
    bool wal_use_direct_io;         /* O_DIRECT for bypassing page cache (default: true) */
    bool wal_use_async_io;          /* Use async I/O if available (default: false) */
    
    /* Integrity - enable for production, disable for max performance */
    bool wal_enable_crc32;          /* CRC32 validation (default: false) */
    bool wal_enable_checksum;       /* Additional checksums (default: false) */
    
    /* File management */
    uint64_t wal_max_file_size;     /* Max WAL size before rotation (default: 1GB) */
    uint32_t wal_max_files;         /* Number of WAL files to keep (default: 10) */
    
    /* =========================================================================
     * ORDERBOOK PERFORMANCE
     * ========================================================================= */
    
    /* 
     * Price ladder optimization:
     * - true: Maintain separate ladders per product (more memory, faster)
     * - false: Shared structure (less memory, slower)
     */
    bool orderbook_per_product_ladder;  /* Separate ladders per product (default: true) */
    
    /* 
     * Memory ordering:
     * - true: Use memory barriers for strict ordering (slower, safer)
     * - false: Allow compiler reordering (faster, may need careful use)
     */
    bool orderbook_strict_memory_order; /* Strict memory ordering (default: false) */
    
    /* =========================================================================
     * MATCHING ENGINE PERFORMANCE
     * ========================================================================= */
    
    /* Batch processing - group multiple operations */
    uint32_t match_batch_size;      /* Orders to batch before matching (default: 1) */
    
    /* 
     * Pre-check for matches before inserting:
     * - true: Check for immediate matches first (lower latency for fills)
     * - false: Always insert then match (simpler, may be faster for high insert rate)
     */
    bool match_pre_check;           /* Pre-check for matches (default: true) */
    
    /* =========================================================================
     * THREADING & CONCURRENCY
     * ========================================================================= */
    
    /* 
     * Thread-local storage:
     * - true: Use thread-local buffers (faster, no locks)
     * - false: Shared buffers (slower, but lower memory)
     */
    bool use_thread_local;          /* Thread-local buffers (default: true) */
    
    /* Number of worker threads for background tasks */
    uint32_t background_threads;    /* Background thread count (default: 2) */
    
} OmPerfConfig;

/* Default performance configuration - balanced for general use */
extern const OmPerfConfig OM_PERF_DEFAULT;

/* High-frequency trading config - max throughput, less durability */
extern const OmPerfConfig OM_PERF_HFT;

/* Maximum durability config - frequent syncs, all checks enabled */
extern const OmPerfConfig OM_PERF_DURABLE;

/* Recovery-focused config - optimized for crash recovery */
extern const OmPerfConfig OM_PERF_RECOVERY;

/* Minimal memory config - low memory footprint */
extern const OmPerfConfig OM_PERF_MINIMAL;

/*
 * Validate performance configuration
 * Returns 0 if valid, -1 if invalid with reason
 */
int om_perf_validate(const OmPerfConfig *config, char *error_buf, size_t error_buf_size);

/*
 * Print performance configuration for debugging
 */
void om_perf_print(const OmPerfConfig *config);

/*
 * Auto-tune configuration based on system capabilities
 * Detects CPU count, memory, disk type and adjusts settings
 */
int om_perf_autotune(OmPerfConfig *config);

#endif
