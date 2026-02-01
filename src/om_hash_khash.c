#include <stdlib.h>
#include <string.h>
#include "khash.h"
#include "../include/openmatch/om_hash.h"

OmHashMap *om_hash_create(size_t initial_capacity) {
    OmHashMap *map = calloc(1, sizeof(OmHashMap));
    if (!map) return NULL;

    map->hash = kh_init(idx);
    if (!map->hash) {
        free(map);
        return NULL;
    }

    if (initial_capacity > 0) {
        kh_resize(idx, map->hash, initial_capacity);
    }

    return map;
}

void om_hash_destroy(OmHashMap *map) {
    if (!map) return;

    if (map->hash) {
        kh_destroy(idx, map->hash);
    }
    free(map);
}

bool om_hash_insert(OmHashMap *map, uint64_t key, uint32_t value) {
    if (!map || !map->hash) return false;

    int ret;
    khiter_t k = kh_put(idx, map->hash, key, &ret);

    if (ret < 0) {
        return false;
    }

    kh_value(map->hash, k) = value;
    return true;
}

uint32_t om_hash_get(OmHashMap *map, uint64_t key) {
    if (!map || !map->hash) return UINT32_MAX;

    khiter_t k = kh_get(idx, map->hash, key);
    if (k == kh_end(map->hash)) {
        return UINT32_MAX;
    }

    return kh_value(map->hash, k);
}

bool om_hash_remove(OmHashMap *map, uint64_t key) {
    if (!map || !map->hash) return false;

    khiter_t k = kh_get(idx, map->hash, key);
    if (k == kh_end(map->hash)) {
        return false;
    }

    kh_del(idx, map->hash, k);
    return true;
}

bool om_hash_contains(OmHashMap *map, uint64_t key) {
    if (!map || !map->hash) return false;

    khiter_t k = kh_get(idx, map->hash, key);
    return k != kh_end(map->hash);
}

size_t om_hash_size(const OmHashMap *map) {
    if (!map || !map->hash) return 0;

    return kh_size(map->hash);
}
