#include "om_perf.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* ============================================================================
 * Predefined Performance Configurations
 * ============================================================================ */

/* Default - balanced for general use */
const OmPerfConfig OM_PERF_DEFAULT = {
    /* Slab */
    .slab_total_slots = 1000000,        /* 1M slots */
    .slab_user_data_size = 64,          /* 64 bytes hot data */
    .slab_aux_data_size = 256,          /* 256 bytes cold data */
    .slab_preallocate = true,
    
    /* Hashmap */
    .hashmap_initial_cap = 1000000,     /* 1M entries */
    .hashmap_load_factor = 0.75f,
    
    /* WAL */
    .wal_buffer_size = 1024 * 1024,     /* 1MB */
    .wal_read_buffer_size = 1024 * 1024,
    .wal_sync_interval_ms = 10,
    .wal_sync_on_insert = false,
    .wal_sync_on_cancel = false,
    .wal_use_direct_io = true,
    .wal_use_async_io = false,
    .wal_enable_crc32 = false,
    .wal_enable_checksum = false,
    .wal_max_file_size = 1024ULL * 1024 * 1024, /* 1GB */
    .wal_max_files = 10,
    
    /* Orderbook */
    .orderbook_per_product_ladder = true,
    .orderbook_strict_memory_order = false,
    
    /* Matching */
    .match_batch_size = 1,
    .match_pre_check = true,
    
    /* Threading */
    .use_thread_local = true,
    .background_threads = 2,
};

/* HFT - max throughput, less durability */
const OmPerfConfig OM_PERF_HFT = {
    .slab_total_slots = 2000000,
    .slab_user_data_size = 64,
    .slab_aux_data_size = 128,          /* Smaller cold data */
    .slab_preallocate = true,
    
    .hashmap_initial_cap = 2000000,
    .hashmap_load_factor = 0.80f,       /* Higher load, less memory */
    
    .wal_buffer_size = 4 * 1024 * 1024, /* 4MB - larger batches */
    .wal_read_buffer_size = 1024 * 1024,
    .wal_sync_interval_ms = 100,        /* Infrequent syncs */
    .wal_sync_on_insert = false,
    .wal_sync_on_cancel = false,
    .wal_use_direct_io = true,
    .wal_use_async_io = true,           /* Async I/O if available */
    .wal_enable_crc32 = false,
    .wal_enable_checksum = false,
    .wal_max_file_size = 2ULL * 1024 * 1024 * 1024,
    .wal_max_files = 5,
    
    .orderbook_per_product_ladder = true,
    .orderbook_strict_memory_order = false,
    
    .match_batch_size = 10,             /* Batch matching */
    .match_pre_check = true,
    
    .use_thread_local = true,
    .background_threads = 4,
};

/* Maximum durability - frequent syncs, all checks */
const OmPerfConfig OM_PERF_DURABLE = {
    .slab_total_slots = 1000000,
    .slab_user_data_size = 64,
    .slab_aux_data_size = 256,
    .slab_preallocate = true,
    
    .hashmap_initial_cap = 1000000,
    .hashmap_load_factor = 0.75f,
    
    .wal_buffer_size = 256 * 1024,      /* Smaller buffer, more frequent writes */
    .wal_read_buffer_size = 1024 * 1024,
    .wal_sync_interval_ms = 1,          /* Frequent syncs */
    .wal_sync_on_insert = true,         /* Sync every insert */
    .wal_sync_on_cancel = false,
    .wal_use_direct_io = true,
    .wal_use_async_io = false,
    .wal_enable_crc32 = true,           /* Enable checks */
    .wal_enable_checksum = true,
    .wal_max_file_size = 512ULL * 1024 * 1024,
    .wal_max_files = 20,
    
    .orderbook_per_product_ladder = true,
    .orderbook_strict_memory_order = true,
    
    .match_batch_size = 1,
    .match_pre_check = true,
    
    .use_thread_local = false,          /* Shared for consistency */
    .background_threads = 1,
};

/* Recovery-focused - optimized for fast recovery */
const OmPerfConfig OM_PERF_RECOVERY = {
    .slab_total_slots = 1000000,
    .slab_user_data_size = 64,
    .slab_aux_data_size = 256,
    .slab_preallocate = true,
    
    .hashmap_initial_cap = 1000000,
    .hashmap_load_factor = 0.60f,       /* Lower load for faster lookups */
    
    .wal_buffer_size = 8 * 1024 * 1024, /* Large buffers for sequential reads */
    .wal_read_buffer_size = 8 * 1024 * 1024,
    .wal_sync_interval_ms = 50,
    .wal_sync_on_insert = false,
    .wal_sync_on_cancel = false,
    .wal_use_direct_io = true,
    .wal_use_async_io = false,
    .wal_enable_crc32 = false,          /* Skip CRC for speed */
    .wal_enable_checksum = false,
    .wal_max_file_size = 2ULL * 1024 * 1024 * 1024,
    .wal_max_files = 3,                 /* Fewer files to scan */
    
    .orderbook_per_product_ladder = true,
    .orderbook_strict_memory_order = false,
    
    .match_batch_size = 100,            /* Large batches for recovery */
    .match_pre_check = false,
    
    .use_thread_local = false,
    .background_threads = 8,            /* Parallel recovery */
};

/* Minimal memory - low footprint */
const OmPerfConfig OM_PERF_MINIMAL = {
    .slab_total_slots = 100000,
    .slab_user_data_size = 32,
    .slab_aux_data_size = 64,
    .slab_preallocate = false,          /* On-demand allocation */
    
    .hashmap_initial_cap = 100000,
    .hashmap_load_factor = 0.90f,       /* High load, minimal memory */
    
    .wal_buffer_size = 64 * 1024,       /* Small buffer */
    .wal_read_buffer_size = 64 * 1024,
    .wal_sync_interval_ms = 100,
    .wal_sync_on_insert = false,
    .wal_sync_on_cancel = false,
    .wal_use_direct_io = false,         /* No O_DIRECT (needs alignment) */
    .wal_use_async_io = false,
    .wal_enable_crc32 = false,
    .wal_enable_checksum = false,
    .wal_max_file_size = 256ULL * 1024 * 1024,
    .wal_max_files = 3,
    
    .orderbook_per_product_ladder = false,
    .orderbook_strict_memory_order = false,
    
    .match_batch_size = 1,
    .match_pre_check = true,
    
    .use_thread_local = false,
    .background_threads = 1,
};

/* ============================================================================
 * Validation
 * ============================================================================ */

int om_perf_validate(const OmPerfConfig *config, char *error_buf, size_t error_buf_size) {
    if (!config) {
        if (error_buf && error_buf_size > 0) {
            snprintf(error_buf, error_buf_size, "Config is NULL");
        }
        return -1;
    }
    
    /* Slab validation */
    if (config->slab_total_slots == 0) {
        snprintf(error_buf, error_buf_size, "slab_total_slots must be > 0");
        return -1;
    }
    
    if (config->slab_total_slots > 100000000) {
        snprintf(error_buf, error_buf_size, "slab_total_slots too large (max 100M)");
        return -1;
    }
    
    /* WAL validation */
    if (config->wal_buffer_size < 4096) {
        snprintf(error_buf, error_buf_size, "wal_buffer_size must be >= 4096");
        return -1;
    }
    
    if (config->wal_use_direct_io && (config->wal_buffer_size % 4096) != 0) {
        snprintf(error_buf, error_buf_size, "wal_buffer_size must be 4KB aligned for O_DIRECT");
        return -1;
    }
    
    /* Hashmap validation */
    if (config->hashmap_load_factor < 0.1f || config->hashmap_load_factor > 0.95f) {
        snprintf(error_buf, error_buf_size, "hashmap_load_factor must be in [0.1, 0.95]");
        return -1;
    }
    
    return 0;
}

/* ============================================================================
 * Printing
 * ============================================================================ */

void om_perf_print(const OmPerfConfig *config) {
    if (!config) {
        printf("Config is NULL\n");
        return;
    }
    
    printf("OpenMatch Performance Configuration:\n");
    printf("====================================\n\n");
    
    printf("[Slab Allocator]\n");
    printf("  Total slots:      %u\n", config->slab_total_slots);
    printf("  User data size:   %zu bytes\n", config->slab_user_data_size);
    printf("  Aux data size:    %zu bytes\n", config->slab_aux_data_size);
    printf("  Preallocate:      %s\n", config->slab_preallocate ? "yes" : "no");
    printf("  Memory usage:     ~%.1f MB\n", 
           (config->slab_total_slots * (64 + config->slab_user_data_size + config->slab_aux_data_size)) / (1024.0 * 1024.0));
    
    printf("\n[Hashmap]\n");
    printf("  Initial capacity: %u\n", config->hashmap_initial_cap);
    printf("  Load factor:      %.2f\n", config->hashmap_load_factor);
    
    printf("\n[WAL]\n");
    printf("  Buffer size:      %zu KB\n", config->wal_buffer_size / 1024);
    printf("  Sync interval:    %u ms\n", config->wal_sync_interval_ms);
    printf("  Sync on insert:   %s\n", config->wal_sync_on_insert ? "yes" : "no");
    printf("  Use O_DIRECT:     %s\n", config->wal_use_direct_io ? "yes" : "no");
    printf("  CRC32 enabled:    %s\n", config->wal_enable_crc32 ? "yes" : "no");
    printf("  Max file size:    %.1f GB\n", config->wal_max_file_size / (1024.0 * 1024 * 1024));
    
    printf("\n[Matching]\n");
    printf("  Batch size:       %u\n", config->match_batch_size);
    printf("  Pre-check:        %s\n", config->match_pre_check ? "yes" : "no");
    
    printf("\n[Threading]\n");
    printf("  Thread-local:     %s\n", config->use_thread_local ? "yes" : "no");
    printf("  Background threads: %u\n", config->background_threads);
}

/* ============================================================================
 * Auto-tuning
 * ============================================================================ */

int om_perf_autotune(OmPerfConfig *config) {
    if (!config) {
        return -1;
    }
    
    /* Start with defaults */
    *config = OM_PERF_DEFAULT;
    
    /* Detect CPU cores */
    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (nproc > 0) {
        config->background_threads = (nproc > 8) ? 8 : (uint32_t)nproc;
    }
    
    /* Detect memory (rough estimate) */
    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    if (pages > 0 && page_size > 0) {
        uint64_t total_mem = (uint64_t)pages * page_size;
        uint64_t usable_mem = total_mem / 4; /* Use 25% of RAM */
        
        /* Adjust slot count based on memory */
        size_t slot_size = 64 + config->slab_user_data_size + config->slab_aux_data_size;
        uint32_t max_slots = (uint32_t)(usable_mem / slot_size);
        
        if (max_slots < 100000) {
            config->slab_total_slots = 100000;
        } else if (max_slots > 10000000) {
            config->slab_total_slots = 10000000;
        } else {
            config->slab_total_slots = max_slots;
        }
        
        config->hashmap_initial_cap = config->slab_total_slots;
    }
    
    return 0;
}
