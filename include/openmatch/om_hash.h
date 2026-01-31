#ifndef OM_HASH_H
#define OM_HASH_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include "../khash.h"

KHASH_MAP_INIT_INT64(ptr, void*)

typedef struct OmHashMap {
    khash_t(ptr) *hash;
} OmHashMap;

OmHashMap *om_hash_create(size_t initial_capacity);
void om_hash_destroy(OmHashMap *map);

bool om_hash_insert(OmHashMap *map, uint64_t key, void *value);
void *om_hash_get(OmHashMap *map, uint64_t key);
bool om_hash_remove(OmHashMap *map, uint64_t key);
bool om_hash_contains(OmHashMap *map, uint64_t key);
size_t om_hash_size(const OmHashMap *map);

#endif
