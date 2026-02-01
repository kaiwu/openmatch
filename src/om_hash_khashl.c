#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

/* Must include om_hash.h first to get the khl_t type definition from KHASHL_MAP_INIT */
#include "../include/openmatch/om_hash.h"

OmHashMap *om_hash_create(size_t initial_capacity) {
    OmHashMap *map = calloc(1, sizeof(OmHashMap));
    if (!map) return NULL;

    map->hash = khl_init();
    if (!map->hash) {
        free(map);
        return NULL;
    }

    if (initial_capacity > 0) {
        khl_resize(map->hash, initial_capacity);
    }

    return map;
}

void om_hash_destroy(OmHashMap *map) {
    if (!map) return;

    if (map->hash) {
        khl_destroy(map->hash);
    }
    free(map);
}

bool om_hash_insert(OmHashMap *map, uint64_t key, uint32_t value) {
    if (!map || !map->hash) return false;

    int ret;
    khint_t idx = khl_put(map->hash, key, &ret);

    if (ret < 0) {
        return false;
    }

    kh_val(map->hash, idx) = value;
    return true;
}

uint32_t om_hash_get(OmHashMap *map, uint64_t key) {
    if (!map || !map->hash) return UINT32_MAX;

    khint_t idx = khl_get(map->hash, key);
    if (idx >= kh_end(map->hash)) {
        return UINT32_MAX;
    }

    return kh_val(map->hash, idx);
}

bool om_hash_remove(OmHashMap *map, uint64_t key) {
    if (!map || !map->hash) return false;

    khint_t idx = khl_get(map->hash, key);
    if (idx >= kh_end(map->hash)) {
        return false;
    }

    khl_del(map->hash, idx);
    return true;
}

bool om_hash_contains(OmHashMap *map, uint64_t key) {
    if (!map || !map->hash) return false;

    khint_t idx = khl_get(map->hash, key);
    return idx < kh_end(map->hash);
}

size_t om_hash_size(const OmHashMap *map) {
    if (!map || !map->hash) return 0;

    return map->hash->count;
}
