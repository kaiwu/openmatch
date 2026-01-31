#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include "openmatch/om_hash.h"

OmHashMap *om_hash_create(size_t initial_capacity) {
    (void)initial_capacity;
    return NULL;
}

void om_hash_destroy(OmHashMap *map) {
    free(map);
}

bool om_hash_insert(OmHashMap *map, uint64_t key, void *value) {
    (void)map; (void)key; (void)value;
    return false;
}

void *om_hash_get(OmHashMap *map, uint64_t key) {
    (void)map; (void)key;
    return NULL;
}

bool om_hash_remove(OmHashMap *map, uint64_t key) {
    (void)map; (void)key;
    return false;
}

bool om_hash_contains(OmHashMap *map, uint64_t key) {
    (void)map; (void)key;
    return false;
}

size_t om_hash_size(const OmHashMap *map) {
    (void)map;
    return 0;
}
