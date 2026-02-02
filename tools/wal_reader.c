#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "openmatch/om_wal.h"

static const char *wal_type_name(OmWalType type) {
    switch (type) {
        case OM_WAL_INSERT: return "INSERT";
        case OM_WAL_CANCEL: return "CANCEL";
        case OM_WAL_MATCH: return "MATCH";
        case OM_WAL_CHECKPOINT: return "CHECKPOINT";
        case OM_WAL_DEACTIVATE: return "DEACTIVATE";
        case OM_WAL_ACTIVATE: return "ACTIVATE";
        default: return "UNKNOWN";
    }
}

static void print_insert(const OmWalInsert *rec) {
    printf("  order_id=%" PRIu64 " price=%" PRIu64 " volume=%" PRIu64 " remain=%" PRIu64
           " org=%" PRIu16 " flags=0x%04" PRIx16 " product=%" PRIu16
           " user=%" PRIu32 " aux=%" PRIu32 " ts=%" PRIu64 "\n",
           rec->order_id, rec->price, rec->volume, rec->vol_remain,
           rec->org, rec->flags, rec->product_id,
           rec->user_data_size, rec->aux_data_size, rec->timestamp_ns);
}

static void print_cancel(const OmWalCancel *rec) {
    printf("  order_id=%" PRIu64 " slot=%" PRIu32 " product=%" PRIu16 " ts=%" PRIu64 "\n",
           rec->order_id, rec->slot_idx, rec->product_id, rec->timestamp_ns);
}

static void print_match(const OmWalMatch *rec) {
    printf("  maker=%" PRIu64 " taker=%" PRIu64 " price=%" PRIu64 " qty=%" PRIu64
           " product=%" PRIu16 " ts=%" PRIu64 "\n",
           rec->maker_id, rec->taker_id, rec->price, rec->volume,
           rec->product_id, rec->timestamp_ns);
}

static void print_deactivate(const OmWalDeactivate *rec) {
    printf("  order_id=%" PRIu64 " slot=%" PRIu32 " product=%" PRIu16 " ts=%" PRIu64 "\n",
           rec->order_id, rec->slot_idx, rec->product_id, rec->timestamp_ns);
}

static void print_activate(const OmWalActivate *rec) {
    printf("  order_id=%" PRIu64 " slot=%" PRIu32 " product=%" PRIu16 " ts=%" PRIu64 "\n",
           rec->order_id, rec->slot_idx, rec->product_id, rec->timestamp_ns);
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <wal_file>\n", argv[0]);
        return 2;
    }

    OmWalReplay replay;
    if (om_wal_replay_init(&replay, argv[1]) != 0) {
        fprintf(stderr, "failed to open wal: %s\n", argv[1]);
        return 1;
    }

    OmWalType type;
    void *data;
    uint64_t sequence;
    size_t data_len;

    while (1) {
        int ret = om_wal_replay_next(&replay, &type, &data, &sequence, &data_len);
        if (ret == 0) {
            break;
        }
        if (ret < 0) {
            fprintf(stderr, "error reading wal (ret=%d)\n", ret);
            break;
        }

        printf("seq=%" PRIu64 " type=%s len=%zu\n", sequence, wal_type_name(type), data_len);

        switch (type) {
            case OM_WAL_INSERT:
                if (data_len >= sizeof(OmWalInsert)) {
                    print_insert((const OmWalInsert *)data);
                }
                break;
            case OM_WAL_CANCEL:
                if (data_len == sizeof(OmWalCancel)) {
                    print_cancel((const OmWalCancel *)data);
                }
                break;
            case OM_WAL_MATCH:
                if (data_len == sizeof(OmWalMatch)) {
                    print_match((const OmWalMatch *)data);
                }
                break;
            case OM_WAL_DEACTIVATE:
                if (data_len == sizeof(OmWalDeactivate)) {
                    print_deactivate((const OmWalDeactivate *)data);
                }
                break;
            case OM_WAL_ACTIVATE:
                if (data_len == sizeof(OmWalActivate)) {
                    print_activate((const OmWalActivate *)data);
                }
                break;
            default:
                break;
        }
    }

    om_wal_replay_close(&replay);
    return 0;
}
