#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include "openmatch/om_wal.h"
#include "openmatch/om_slab.h"
#include "openmatch/orderbook.h"

/* Simple xorshift64 PRNG — fast, portable, no library dependency */
static uint64_t rng_state = 0;

static uint64_t rng_next(void) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}

static uint64_t rng_range(uint64_t lo, uint64_t hi) {
    return lo + rng_next() % (hi - lo + 1);
}

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s [options] <output_wal>\n"
        "\n"
        "Generate a WAL file with random records for testing.\n"
        "\n"
        "options:\n"
        "  -n count      Number of records to generate (default 100)\n"
        "  -e count      Number of records to corrupt after writing (default 0)\n"
        "  -c            Enable CRC32 (required for -e to be useful)\n"
        "  -p products   Number of product IDs 0..N-1 (default 4)\n"
        "  -S seed       RNG seed (default: from clock)\n"
        "\n"
        "Record mix: ~50%% INSERT, ~15%% CANCEL, ~15%% MATCH,\n"
        "            ~10%% DEACTIVATE, ~10%% ACTIVATE\n"
        "\n"
        "examples:\n"
        "  %s -n 1000 -c /tmp/test.wal\n"
        "  %s -n 500 -c -e 3 /tmp/broken.wal\n",
        prog, prog, prog);
}

/* Corrupt random payload bytes in `count` distinct records in the WAL file.
 * Only works on CRC-enabled WALs (otherwise there's nothing to detect). */
static int corrupt_records(const char *path, int count, int total_records) {
    if (count <= 0) return 0;
    if (count > total_records) count = total_records;

    int fd = open(path, O_RDWR);
    if (fd < 0) {
        fprintf(stderr, "cannot open %s for corruption\n", path);
        return -1;
    }

    /* Pick `count` distinct record indices to corrupt */
    bool *chosen = calloc((size_t)total_records, sizeof(bool));
    if (!chosen) { close(fd); return -1; }

    int picked = 0;
    while (picked < count) {
        int idx = (int)(rng_next() % (uint64_t)total_records);
        if (!chosen[idx]) {
            chosen[idx] = true;
            picked++;
        }
    }

    /* Walk through the file record-by-record, corrupt chosen ones */
    off_t offset = 0;
    int corrupted = 0;
    for (int i = 0; i < total_records; i++) {
        /* Read header */
        OmWalHeader hdr;
        if (pread(fd, &hdr, sizeof(hdr), offset) != sizeof(hdr)) break;

        uint16_t payload_len = om_wal_header_len(hdr.seq_type_len);
        uint64_t seq = om_wal_header_seq(hdr.seq_type_len);

        /* On-disk record: header(8) + payload(payload_len) + CRC(4) */
        size_t record_size = sizeof(OmWalHeader) + payload_len + 4;

        if (chosen[i] && payload_len > 0) {
            /* Flip a byte in the payload area */
            off_t flip_off = offset + (off_t)sizeof(OmWalHeader)
                           + (off_t)(rng_next() % payload_len);
            uint8_t byte;
            pread(fd, &byte, 1, flip_off);
            byte ^= 0xFF;
            pwrite(fd, &byte, 1, flip_off);
            corrupted++;
            fprintf(stderr, "corrupted record seq %" PRIu64
                    " at file offset %" PRId64 "\n", seq, (int64_t)offset);
        }

        offset += (off_t)record_size;
    }

    free(chosen);
    close(fd);
    fprintf(stderr, "corrupted %d/%d records\n", corrupted, count);
    return 0;
}

int main(int argc, char **argv) {
    int n_records = 100;
    int n_corrupt = 0;
    bool enable_crc = false;
    int n_products = 4;
    uint64_t seed = 0;
    bool seed_set = false;
    const char *output = NULL;

    int opt;
    while ((opt = getopt(argc, argv, "n:e:cp:S:")) != -1) {
        switch (opt) {
            case 'n':
                n_records = atoi(optarg);
                if (n_records <= 0) {
                    fprintf(stderr, "invalid -n: %s\n", optarg);
                    return 2;
                }
                break;
            case 'e':
                n_corrupt = atoi(optarg);
                if (n_corrupt < 0) {
                    fprintf(stderr, "invalid -e: %s\n", optarg);
                    return 2;
                }
                break;
            case 'c':
                enable_crc = true;
                break;
            case 'p':
                n_products = atoi(optarg);
                if (n_products <= 0) {
                    fprintf(stderr, "invalid -p: %s\n", optarg);
                    return 2;
                }
                break;
            case 'S':
                seed = strtoull(optarg, NULL, 10);
                seed_set = true;
                break;
            default:
                usage(argv[0]);
                return 2;
        }
    }

    if (optind >= argc) {
        usage(argv[0]);
        return 2;
    }
    output = argv[optind];

    if (n_corrupt > 0 && !enable_crc) {
        fprintf(stderr, "warning: -e without -c has no effect (no CRC to break)\n");
    }

    /* Seed RNG */
    if (!seed_set) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        seed = (uint64_t)ts.tv_nsec ^ ((uint64_t)ts.tv_sec << 32);
    }
    rng_state = seed ? seed : 1;
    fprintf(stderr, "seed: %" PRIu64 "\n", rng_state);

    /* Remove old file */
    unlink(output);

    /* Init slab + WAL */
    OmSlabConfig slab_cfg = { .total_slots = (size_t)(n_records + 1) };
    OmWalConfig wal_cfg = {
        .filename = output,
        .buffer_size = 1024 * 1024,
        .use_direct_io = false,
        .enable_crc32 = enable_crc,
    };

    OmWal wal;
    if (om_wal_init(&wal, &wal_cfg) != 0) {
        fprintf(stderr, "failed to init wal: %s\n", output);
        return 1;
    }

    OmOrderbookContext ctx;
    if (om_orderbook_init(&ctx, &slab_cfg, &wal, (uint16_t)n_products, 1000, 0) != 0) {
        fprintf(stderr, "failed to init orderbook\n");
        om_wal_close(&wal);
        return 1;
    }

    /* Track live orders for cancel/match/deactivate/activate targets */
    uint32_t *live_oids = calloc((size_t)n_records, sizeof(uint32_t));
    uint16_t *live_pids = calloc((size_t)n_records, sizeof(uint16_t));
    uint32_t *live_slots = calloc((size_t)n_records, sizeof(uint32_t));
    int n_live = 0;

    int counts[6] = {0}; /* INSERT, CANCEL, MATCH, DEACTIVATE, ACTIVATE, other */

    for (int i = 0; i < n_records; i++) {
        uint16_t pid = (uint16_t)(rng_next() % (uint64_t)n_products);
        int roll = (int)(rng_next() % 100);

        if (n_live == 0 || roll < 50) {
            /* INSERT */
            uint32_t oid = om_slab_next_order_id(&ctx.slab);
            OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
            if (!slot) {
                fprintf(stderr, "slab full at record %d\n", i);
                break;
            }
            slot->order_id = oid;
            slot->price = rng_range(9000, 11000);
            slot->volume = rng_range(1, 100);
            slot->volume_remain = slot->volume;
            slot->org = (uint16_t)rng_range(1, 10);
            slot->flags = (rng_next() & 1) ? OM_SIDE_BID : OM_SIDE_ASK;
            om_wal_insert(&wal, slot, pid);
            live_oids[n_live] = oid;
            live_pids[n_live] = pid;
            live_slots[n_live] = om_slot_get_idx(&ctx.slab, slot);
            n_live++;
            counts[0]++;
        } else if (roll < 65 && n_live > 0) {
            /* CANCEL */
            int idx = (int)(rng_next() % (uint64_t)n_live);
            om_wal_cancel(&wal, live_oids[idx], live_slots[idx], live_pids[idx]);
            live_oids[idx] = live_oids[n_live - 1];
            live_pids[idx] = live_pids[n_live - 1];
            live_slots[idx] = live_slots[n_live - 1];
            n_live--;
            counts[1]++;
        } else if (roll < 80 && n_live >= 2) {
            /* MATCH */
            int m = (int)(rng_next() % (uint64_t)n_live);
            int t = (int)(rng_next() % (uint64_t)n_live);
            if (t == m) t = (t + 1) % n_live;
            OmWalMatch match = {
                .maker_id = live_oids[m],
                .taker_id = live_oids[t],
                .price = rng_range(9000, 11000),
                .volume = rng_range(1, 50),
                .product_id = live_pids[m],
            };
            om_wal_match(&wal, &match);
            counts[2]++;
        } else if (roll < 90 && n_live > 0) {
            /* DEACTIVATE */
            int idx = (int)(rng_next() % (uint64_t)n_live);
            om_wal_deactivate(&wal, live_oids[idx], live_slots[idx], live_pids[idx]);
            counts[3]++;
        } else if (n_live > 0) {
            /* ACTIVATE */
            int idx = (int)(rng_next() % (uint64_t)n_live);
            om_wal_activate(&wal, live_oids[idx], live_slots[idx], live_pids[idx]);
            counts[4]++;
        } else {
            /* Fallback to INSERT */
            uint32_t oid = om_slab_next_order_id(&ctx.slab);
            OmSlabSlot *slot = om_slab_alloc(&ctx.slab);
            if (!slot) break;
            slot->order_id = oid;
            slot->price = rng_range(9000, 11000);
            slot->volume = rng_range(1, 100);
            slot->volume_remain = slot->volume;
            slot->org = (uint16_t)rng_range(1, 10);
            slot->flags = (rng_next() & 1) ? OM_SIDE_BID : OM_SIDE_ASK;
            om_wal_insert(&wal, slot, pid);
            live_oids[n_live] = oid;
            live_pids[n_live] = pid;
            live_slots[n_live] = om_slot_get_idx(&ctx.slab, slot);
            n_live++;
            counts[0]++;
        }
    }

    om_wal_flush(&wal);
    om_wal_close(&wal);
    om_orderbook_destroy(&ctx);

    fprintf(stderr, "wrote %s: %d INSERT, %d CANCEL, %d MATCH, "
            "%d DEACTIVATE, %d ACTIVATE (total %d)\n",
            output, counts[0], counts[1], counts[2], counts[3], counts[4],
            counts[0] + counts[1] + counts[2] + counts[3] + counts[4]);

    /* Corrupt if requested */
    if (n_corrupt > 0 && enable_crc) {
        int total = counts[0] + counts[1] + counts[2] + counts[3] + counts[4];
        corrupt_records(output, n_corrupt, total);
    }

    free(live_oids);
    free(live_pids);
    free(live_slots);

    return 0;
}
