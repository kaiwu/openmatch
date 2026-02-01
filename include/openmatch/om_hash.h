#ifndef OM_HASH_H
#define OM_HASH_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef OM_USE_KHASHL
#include "khashl.h"
/* Instantiate khashl hash table type with uint64_t keys and uint32_t values (slot indices).
 * This must be in the header so all users see the complete type definition.
 * Functions are declared static to avoid multiple definition errors. */
KHASHL_MAP_INIT(static, khl_t, khl, uint64_t, uint32_t, kh_hash_uint64, kh_eq_generic)
typedef struct OmHashMap {
    khl_t *hash;
} OmHashMap;
/* Compatibility macros for khash API - khashl already provides kh_val, kh_key, kh_end, kh_exist */
#define khiter_t khint_t
#define kh_begin(h) 0
#define kh_value(h, x) kh_val(h, x)
#else
#include "khash.h"
KHASH_MAP_INIT_INT64(idx, uint32_t)
typedef struct OmHashMap {
    khash_t(idx) *hash;
} OmHashMap;
#endif

OmHashMap *om_hash_create(size_t initial_capacity);
void om_hash_destroy(OmHashMap *map);

bool om_hash_insert(OmHashMap *map, uint64_t key, uint32_t value);
uint32_t om_hash_get(OmHashMap *map, uint64_t key);
bool om_hash_remove(OmHashMap *map, uint64_t key);
bool om_hash_contains(OmHashMap *map, uint64_t key);
size_t om_hash_size(const OmHashMap *map);

#endif
