/**
 * @file om_hash_f14.c
 * @brief F14 SIMD-accelerated hash map implementation
 * 
 * F14 (Facebook's 14-slot probing) uses SIMD to check 14 slots at once,
 * providing ~2-3x better performance than traditional probing on x86_64.
 * 
 * This implementation requires SSE2 (available on all x86_64 CPUs).
 * Compile with -msse2 or -march=x86-64 (default on most compilers).
 */

#ifdef OM_USE_F14

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <emmintrin.h>  /* SSE2 */
#include "../include/openmatch/om_hash.h"

/* F14 uses 14 slots per 64-byte chunk */
#define F14_SLOTS_PER_CHUNK 14
#define F14_CHUNK_SIZE 64

/* Metadata byte values */
#define F14_EMPTY 0x80    /* 10000000 - empty slot */
#define F14_DELETED 0x40  /* 01000000 - deleted slot */
#define F14_VALUE_MASK 0x3F  /* 00111111 - value mask (0-63) */

/* Slot structure - packed to fit 14 in 64 bytes with metadata */
typedef struct {
    uint64_t key;       /* 8 bytes */
    PriceLevel value;   /* 8 bytes (head + tail as uint32_t each) */
} F14Slot;

/* Ensure slot is exactly 16 bytes */
static_assert(sizeof(F14Slot) == 16, "F14Slot must be 16 bytes");

/* Chunk structure: 14 slots (224 bytes) + 14 metadata bytes + padding = 256 bytes
 * Actually, we use 64-byte chunks with 14 metadata bytes + 50 bytes padding
 * This fits better with cache lines */
typedef struct {
    uint8_t metadata[F14_SLOTS_PER_CHUNK];  /* 14 bytes */
    uint8_t pad[50];                         /* 50 bytes padding to 64 */
} F14ChunkMeta;

typedef struct {
    F14Slot slots[F14_SLOTS_PER_CHUNK];     /* 14 slots = 224 bytes */
} F14ChunkSlots;

typedef struct {
    F14ChunkMeta meta;   /* 64 bytes, cache line aligned */
    F14ChunkSlots data;  /* 224 bytes */
} F14Chunk;

/* Hash map structure */
struct PriceLevelMap {
    F14Chunk *chunks;       /* Array of chunks */
    uint32_t num_chunks;    /* Number of chunks (power of 2) */
    uint32_t size;          /* Number of entries */
    uint32_t mask;          /* num_chunks - 1 for fast modulo */
};

/* Hash function - splitmix64 for good distribution */
static inline uint64_t f14_hash(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
}

/* Extract 7-bit hash tag from full hash for metadata */
static inline uint8_t f14_tag(uint64_t hash) {
    return (uint8_t)(hash & F14_VALUE_MASK);
}

PriceLevelMap* om_hash_create(uint32_t initial_capacity) {
    PriceLevelMap *map = calloc(1, sizeof(PriceLevelMap));
    if (!map) return NULL;
    
    /* Start with at least 8 chunks */
    uint32_t num_chunks = 8;
    while (num_chunks * F14_SLOTS_PER_CHUNK < initial_capacity) {
        num_chunks <<= 1;
    }
    
    map->chunks = aligned_alloc(64, num_chunks * sizeof(F14Chunk));
    if (!map->chunks) {
        free(map);
        return NULL;
    }
    
    /* Initialize all metadata to EMPTY */
    memset(map->chunks, 0x80, num_chunks * sizeof(F14Chunk));
    
    map->num_chunks = num_chunks;
    map->mask = num_chunks - 1;
    map->size = 0;
    
    return map;
}

void om_hash_destroy(PriceLevelMap* map) {
    if (!map) return;
    if (map->chunks) free(map->chunks);
    free(map);
}

/* SIMD probe: check 14 slots at once using SSE2 */
static inline int f14_simd_probe(uint8_t tag, const uint8_t *metadata) {
    /* Load 16 bytes (we only use first 14) */
    __m128i md = _mm_loadu_si128((__m128i const*)metadata);
    
    /* Broadcast tag to all positions */
    __m128i t = _mm_set1_epi8((char)tag);
    
    /* Compare for equality */
    __m128i eq = _mm_cmpeq_epi8(md, t);
    
    /* Also check for empty slots (0x80) */
    __m128i empty_mask = _mm_set1_epi8((char)F14_EMPTY);
    __m128i empty = _mm_cmpeq_epi8(md, empty_mask);
    
    /* Combine results */
    __m128i result = _mm_or_si128(eq, empty);
    
    /* Create mask of matching positions */
    int mask = _mm_movemask_epi8(result);
    
    /* Only consider first 14 bits */
    mask &= 0x3FFF;
    
    return mask;
}

bool om_hash_get(PriceLevelMap* map, uint64_t price, PriceLevel* level_out) {
    if (!map || !map->chunks) return false;
    
    uint64_t hash = f14_hash(price);
    uint8_t tag = f14_tag(hash);
    uint32_t chunk_idx = (uint32_t)(hash & map->mask);
    
    /* Probe chunks until we find the key or an empty slot */
    for (uint32_t probe = 0; probe < map->num_chunks; probe++) {
        uint32_t idx = (chunk_idx + probe) & map->mask;
        F14Chunk *chunk = &map->chunks[idx];
        
        /* SIMD probe - check all 14 slots at once */
        int match_mask = f14_simd_probe(tag, chunk->meta.metadata);
        
        /* Check each potential match */
        while (match_mask != 0) {
            int slot = __builtin_ctz(match_mask);
            match_mask &= ~(1 << slot);
            
            uint8_t meta = chunk->meta.metadata[slot];
            
            /* Empty slot - key not found */
            if (meta == F14_EMPTY) {
                return false;
            }
            
            /* Check if tag matches and key matches */
            if ((meta & F14_VALUE_MASK) == tag) {
                if (chunk->data.slots[slot].key == price) {
                    if (level_out) {
                        *level_out = chunk->data.slots[slot].value;
                    }
                    return true;
                }
            }
        }
        
        /* Check if any empty slot in this chunk - if so, key not present */
        __m128i md = _mm_loadu_si128((__m128i const*)chunk->meta.metadata);
        __m128i empty_mask = _mm_set1_epi8((char)F14_EMPTY);
        __m128i empty = _mm_cmpeq_epi8(md, empty_mask);
        int empty_mask_bits = _mm_movemask_epi8(empty) & 0x3FFF;
        
        if (empty_mask_bits != 0) {
            return false;  /* Found empty slot, key not in table */
        }
    }
    
    return false;
}

int om_hash_put(PriceLevelMap* map, uint64_t price, const PriceLevel* level) {
    if (!map || !map->chunks || !level) return -1;
    
    /* Check if we need to grow (load factor > 0.9) */
    if (map->size * 10 >= map->num_chunks * F14_SLOTS_PER_CHUNK * 9) {
        /* TODO: Implement resize */
        return -1;
    }
    
    uint64_t hash = f14_hash(price);
    uint8_t tag = f14_tag(hash);
    uint32_t chunk_idx = (uint32_t)(hash & map->mask);
    
    /* Find insert position */
    for (uint32_t probe = 0; probe < map->num_chunks; probe++) {
        uint32_t idx = (chunk_idx + probe) & map->mask;
        F14Chunk *chunk = &map->chunks[idx];
        
        /* Check for empty slot or existing key using SIMD */
        int empty_mask_bits = f14_simd_probe(tag, chunk->meta.metadata);
        
        while (empty_mask_bits != 0) {
            int slot = __builtin_ctz(empty_mask_bits);
            empty_mask_bits &= ~(1 << slot);
            
            uint8_t meta = chunk->meta.metadata[slot];
            
            /* Empty or deleted slot - can insert here */
            if (meta == F14_EMPTY || meta == F14_DELETED) {
                chunk->data.slots[slot].key = price;
                chunk->data.slots[slot].value = *level;
                chunk->meta.metadata[slot] = tag;
                map->size++;
                return 0;
            }
            
            /* Existing key - update value */
            if ((meta & F14_VALUE_MASK) == tag && chunk->data.slots[slot].key == price) {
                chunk->data.slots[slot].value = *level;
                return 0;
            }
        }
    }
    
    return -1;  /* Table full */
}

bool om_hash_del(PriceLevelMap* map, uint64_t price) {
    if (!map || !map->chunks) return false;
    
    uint64_t hash = f14_hash(price);
    uint8_t tag = f14_tag(hash);
    uint32_t chunk_idx = (uint32_t)(hash & map->mask);
    
    for (uint32_t probe = 0; probe < map->num_chunks; probe++) {
        uint32_t idx = (chunk_idx + probe) & map->mask;
        F14Chunk *chunk = &map->chunks[idx];
        
        /* SIMD probe */
        int match_mask = f14_simd_probe(tag, chunk->meta.metadata);
        
        while (match_mask != 0) {
            int slot = __builtin_ctz(match_mask);
            match_mask &= ~(1 << slot);
            
            uint8_t meta = chunk->meta.metadata[slot];
            
            if (meta == F14_EMPTY) {
                return false;
            }
            
            if ((meta & F14_VALUE_MASK) == tag && chunk->data.slots[slot].key == price) {
                chunk->meta.metadata[slot] = F14_DELETED;
                map->size--;
                return true;
            }
        }
        
        /* Check for empty slot */
        __m128i md = _mm_loadu_si128((__m128i const*)chunk->meta.metadata);
        __m128i empty_mask = _mm_set1_epi8((char)F14_EMPTY);
        __m128i empty = _mm_cmpeq_epi8(md, empty_mask);
        int empty_mask_bits = _mm_movemask_epi8(empty) & 0x3FFF;
        
        if (empty_mask_bits != 0) {
            return false;
        }
    }
    
    return false;
}

bool om_hash_contains(PriceLevelMap* map, uint64_t price) {
    return om_hash_get(map, price, NULL);
}

uint32_t om_hash_size(const PriceLevelMap* map) {
    if (!map) return 0;
    return map->size;
}

void om_hash_clear(PriceLevelMap* map) {
    if (!map || !map->chunks) return;
    memset(map->chunks, 0x80, map->num_chunks * sizeof(F14Chunk));
    map->size = 0;
}

uint64_t om_hash_next(PriceLevelMap* map, uint64_t price, PriceLevel* level_out) {
    if (!map || !map->chunks) return UINT64_MAX;
    
    /* Simple iteration - scan all chunks */
    for (uint32_t i = 0; i < map->num_chunks; i++) {
        F14Chunk *chunk = &map->chunks[i];
        for (int j = 0; j < F14_SLOTS_PER_CHUNK; j++) {
            uint8_t meta = chunk->meta.metadata[j];
            if (meta != F14_EMPTY && meta != F14_DELETED) {
                if (price == 0 || chunk->data.slots[j].key > price) {
                    if (level_out) {
                        *level_out = chunk->data.slots[j].value;
                    }
                    return chunk->data.slots[j].key;
                }
            }
        }
    }
    
    return UINT64_MAX;
}

#endif /* OM_USE_F14 */
