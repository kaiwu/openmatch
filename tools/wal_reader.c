#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include "openmatch/om_wal.h"
#include "openmatch/om_error.h"
#include "ombus/om_bus.h"

#define MAX_RANGES 32

typedef struct {
    uint64_t from;
    uint64_t to;
} SeqRange;

typedef struct {
    uint64_t from_ns;
    uint64_t to_ns;
} TimeRange;

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

static void format_timestamp(uint64_t timestamp_ns, char *buf, size_t buf_len) {
    time_t secs = (time_t)(timestamp_ns / 1000000000ULL);
    long nsec = (long)(timestamp_ns % 1000000000ULL);
    struct tm tm_val;
    if (localtime_r(&secs, &tm_val) == NULL) {
        snprintf(buf, buf_len, "0");
        return;
    }
    int len = strftime(buf, buf_len, "%Y-%m-%d %H:%M:%S", &tm_val);
    if (len <= 0 || (size_t)len >= buf_len) {
        snprintf(buf, buf_len, "0");
        return;
    }
    snprintf(buf + len, buf_len - (size_t)len, ".%06ld", nsec / 1000L);
}

static void print_insert(FILE *out, const OmWalInsert *rec, bool format_ts) {
    char ts_buf[64];
    if (format_ts) {
        format_timestamp(rec->timestamp_ns, ts_buf, sizeof(ts_buf));
    }
    fprintf(out, "ts[");
    if (format_ts) {
        fprintf(out, "%s", ts_buf);
    } else {
        fprintf(out, "%" PRIu64, rec->timestamp_ns);
    }
    fprintf(out, "] oid[%" PRIu64 "] p[%" PRIu64 "] v[%" PRIu64 "] vr[%" PRIu64 "] org[%" PRIu16 "] "
           "f[0x%04" PRIx16 "] pid[%" PRIu16 "] ud[%" PRIu32 "] ad[%" PRIu32 "]",
           rec->order_id, rec->price, rec->volume, rec->vol_remain,
           rec->org, rec->flags, rec->product_id,
           rec->user_data_size, rec->aux_data_size);
}

static void print_cancel(FILE *out, const OmWalCancel *rec, bool format_ts) {
    char ts_buf[64];
    if (format_ts) {
        format_timestamp(rec->timestamp_ns, ts_buf, sizeof(ts_buf));
    }
    fprintf(out, "ts[");
    if (format_ts) {
        fprintf(out, "%s", ts_buf);
    } else {
        fprintf(out, "%" PRIu64, rec->timestamp_ns);
    }
    fprintf(out, "] oid[%" PRIu64 "] s[%" PRIu32 "] pid[%" PRIu16 "]",
           rec->order_id, rec->slot_idx, rec->product_id);
}

static void print_match(FILE *out, const OmWalMatch *rec, bool format_ts) {
    char ts_buf[64];
    if (format_ts) {
        format_timestamp(rec->timestamp_ns, ts_buf, sizeof(ts_buf));
    }
    fprintf(out, "ts[");
    if (format_ts) {
        fprintf(out, "%s", ts_buf);
    } else {
        fprintf(out, "%" PRIu64, rec->timestamp_ns);
    }
    fprintf(out, "] m[%" PRIu64 "] t[%" PRIu64 "] p[%" PRIu64 "] q[%" PRIu64 "] pid[%" PRIu16 "]",
           rec->maker_id, rec->taker_id, rec->price, rec->volume,
           rec->product_id);
}

static void print_deactivate(FILE *out, const OmWalDeactivate *rec, bool format_ts) {
    char ts_buf[64];
    if (format_ts) {
        format_timestamp(rec->timestamp_ns, ts_buf, sizeof(ts_buf));
    }
    fprintf(out, "ts[");
    if (format_ts) {
        fprintf(out, "%s", ts_buf);
    } else {
        fprintf(out, "%" PRIu64, rec->timestamp_ns);
    }
    fprintf(out, "] oid[%" PRIu64 "] s[%" PRIu32 "] pid[%" PRIu16 "]",
           rec->order_id, rec->slot_idx, rec->product_id);
}

static void print_activate(FILE *out, const OmWalActivate *rec, bool format_ts) {
    char ts_buf[64];
    if (format_ts) {
        format_timestamp(rec->timestamp_ns, ts_buf, sizeof(ts_buf));
    }
    fprintf(out, "ts[");
    if (format_ts) {
        fprintf(out, "%s", ts_buf);
    } else {
        fprintf(out, "%" PRIu64, rec->timestamp_ns);
    }
    fprintf(out, "] oid[%" PRIu64 "] s[%" PRIu32 "] pid[%" PRIu16 "]",
           rec->order_id, rec->slot_idx, rec->product_id);
}

/* Parse "from-to" sequence range, e.g. "100-200" */
static bool parse_seq_range(const char *arg, SeqRange *r) {
    char *dash = strchr(arg, '-');
    if (!dash || dash == arg) return false;
    r->from = strtoull(arg, NULL, 10);
    r->to = strtoull(dash + 1, NULL, 10);
    return r->from <= r->to;
}

/* Parse YYYYMMDDHHMMSS string to nanoseconds since epoch */
static uint64_t parse_time_str(const char *str, size_t len) {
    if (len != 14) return 0;
    char buf[15];
    memcpy(buf, str, 14);
    buf[14] = '\0';

    struct tm tm_val;
    memset(&tm_val, 0, sizeof(tm_val));
    /* Manual parse for portability (strptime not in C11 standard) */
    int n = 0;
    n = sscanf(buf, "%4d%2d%2d%2d%2d%2d",
               &tm_val.tm_year, &tm_val.tm_mon, &tm_val.tm_mday,
               &tm_val.tm_hour, &tm_val.tm_min, &tm_val.tm_sec);
    if (n != 6) return 0;
    tm_val.tm_year -= 1900;
    tm_val.tm_mon -= 1;
    tm_val.tm_isdst = -1;

    time_t t = mktime(&tm_val);
    if (t == (time_t)-1) return 0;
    return (uint64_t)t * 1000000000ULL;
}

/* Parse "YYYYMMDDHHMMSS-YYYYMMDDHHMMSS" time range */
static bool parse_time_range(const char *arg, TimeRange *r) {
    size_t len = strlen(arg);
    if (len != 29) return false;    /* 14 + 1 + 14 */
    if (arg[14] != '-') return false;

    r->from_ns = parse_time_str(arg, 14);
    r->to_ns = parse_time_str(arg + 15, 14);
    if (r->from_ns == 0 || r->to_ns == 0) return false;
    /* Inclusive: extend to_ns to end of that second */
    r->to_ns += 999999999ULL;
    return r->from_ns <= r->to_ns;
}

/* Extract timestamp_ns from a WAL record by type */
static bool get_record_timestamp(OmWalType type, const void *data,
                                 size_t data_len, uint64_t *ts_out) {
    switch (type) {
        case OM_WAL_INSERT:
            if (data_len >= sizeof(OmWalInsert)) {
                *ts_out = ((const OmWalInsert *)data)->timestamp_ns;
                return true;
            }
            break;
        case OM_WAL_CANCEL:
            if (data_len >= sizeof(OmWalCancel)) {
                *ts_out = ((const OmWalCancel *)data)->timestamp_ns;
                return true;
            }
            break;
        case OM_WAL_MATCH:
            if (data_len >= sizeof(OmWalMatch)) {
                *ts_out = ((const OmWalMatch *)data)->timestamp_ns;
                return true;
            }
            break;
        case OM_WAL_DEACTIVATE:
            if (data_len >= sizeof(OmWalDeactivate)) {
                *ts_out = ((const OmWalDeactivate *)data)->timestamp_ns;
                return true;
            }
            break;
        case OM_WAL_ACTIVATE:
            if (data_len >= sizeof(OmWalActivate)) {
                *ts_out = ((const OmWalActivate *)data)->timestamp_ns;
                return true;
            }
            break;
        default:
            break;
    }
    return false;
}

/* Check if a record matches any filter. No filters = match all. */
static bool record_matches(uint64_t seq, OmWalType type, const void *data,
                           size_t data_len,
                           const SeqRange *seq_ranges, int n_seq,
                           const TimeRange *time_ranges, int n_time) {
    if (n_seq == 0 && n_time == 0) return true;

    for (int i = 0; i < n_seq; i++) {
        if (seq >= seq_ranges[i].from && seq <= seq_ranges[i].to)
            return true;
    }

    uint64_t ts;
    if (n_time > 0 && get_record_timestamp(type, data, data_len, &ts)) {
        for (int i = 0; i < n_time; i++) {
            if (ts >= time_ranges[i].from_ns && ts <= time_ranges[i].to_ns)
                return true;
        }
    }

    return false;
}

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s [options] <wal_file> [wal_file ...]\n"
        "\n"
        "options:\n"
        "  -t                Format timestamps as human-readable\n"
        "  -c                Strict CRC: stop on first corruption\n"
        "                    (without -c, CRC errors are warned but skipped)\n"
        "  -s from-to        Sequence range filter (inclusive, repeatable)\n"
        "  -r from-to        Time range filter (inclusive, repeatable)\n"
        "                    Format: YYYYMMDDHHMMSS-YYYYMMDDHHMMSS\n"
        "  -p stream_name    Replay matching records to SHM bus stream\n"
        "\n"
        "Multiple WAL files are processed in order. Shell glob works:\n"
        "  %s -s 1-100 /tmp/wal_*.log\n"
        "\n"
        "Multiple -s and -r filters form OR logic: a record is included\n"
        "if it falls within ANY specified range.\n"
        "\n"
        "With -p, output goes to stderr and records are published to\n"
        "the named SHM bus stream for downstream consumers.\n",
        prog, prog);
}

int main(int argc, char **argv) {
    bool format_ts = false;
    bool strict_crc = false;
    const char *stream_name = NULL;

    SeqRange seq_ranges[MAX_RANGES];
    TimeRange time_ranges[MAX_RANGES];
    int n_seq = 0;
    int n_time = 0;

    int opt;
    while ((opt = getopt(argc, argv, "tcs:r:p:")) != -1) {
        switch (opt) {
            case 't':
                format_ts = true;
                break;
            case 'c':
                strict_crc = true;
                break;
            case 's':
                if (n_seq >= MAX_RANGES) {
                    fprintf(stderr, "too many -s ranges (max %d)\n", MAX_RANGES);
                    return 2;
                }
                if (!parse_seq_range(optarg, &seq_ranges[n_seq])) {
                    fprintf(stderr, "invalid sequence range: %s\n", optarg);
                    return 2;
                }
                n_seq++;
                break;
            case 'r':
                if (n_time >= MAX_RANGES) {
                    fprintf(stderr, "too many -r ranges (max %d)\n", MAX_RANGES);
                    return 2;
                }
                if (!parse_time_range(optarg, &time_ranges[n_time])) {
                    fprintf(stderr, "invalid time range: %s (expected YYYYMMDDHHMMSS-YYYYMMDDHHMMSS)\n", optarg);
                    return 2;
                }
                n_time++;
                break;
            case 'p':
                stream_name = optarg;
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

    int n_files = argc - optind;

    /* When replaying, render to stderr so stdout stays clean for piping */
    FILE *out = stream_name ? stderr : stdout;

    /* Create SHM bus stream if replay requested */
    OmBusStream *stream = NULL;
    if (stream_name) {
        OmBusStreamConfig cfg = {
            .stream_name = stream_name,
            .capacity = OM_BUS_DEFAULT_CAPACITY,
            .slot_size = OM_BUS_DEFAULT_SLOT_SIZE,
            .max_consumers = OM_BUS_DEFAULT_MAX_CONSUMERS,
        };
        int rc = om_bus_stream_create(&stream, &cfg);
        if (rc != 0) {
            fprintf(stderr, "failed to create bus stream '%s' (rc=%d)\n",
                    stream_name, rc);
            return 1;
        }
        fprintf(stderr, "replaying to SHM stream '%s'\n", stream_name);
    }

    OmWalType type;
    void *data;
    uint64_t sequence;
    size_t data_len;
    uint64_t rendered = 0;
    uint64_t replayed = 0;
    int exit_code = 0;

    for (int fi = 0; fi < n_files; fi++) {
        const char *wal_path = argv[optind + fi];

        OmWalReplay replay;
        int init_rc = om_wal_replay_init(&replay, wal_path);
        if (init_rc != 0) {
            fprintf(stderr, "failed to open wal: %s\n", wal_path);
            exit_code = 1;
            continue;
        }

        while (1) {
            int ret = om_wal_replay_next(&replay, &type, &data, &sequence, &data_len);
            if (ret == 0) {
                break;
            }
            if (ret == OM_ERR_WAL_CRC_MISMATCH) {
                fprintf(stderr,
                    "CRC MISMATCH in %s at seq %" PRIu64 "\n"
                    "  file offset:  %" PRIu64 " (0x%" PRIx64 ")\n"
                    "  stored CRC:   0x%08" PRIx32 " (bad)\n"
                    "  computed CRC: 0x%08" PRIx32 " (good)\n"
                    "  record type:  %s  len: %zu\n"
                    "\n"
                    "To repair, patch 4 bytes at offset %" PRIu64 " + 8 + %zu = %" PRIu64
                    " (0x%" PRIx64 ") with the good CRC value.\n",
                    wal_path, sequence,
                    replay.last_record_offset, replay.last_record_offset,
                    replay.last_stored_crc,
                    replay.last_computed_crc,
                    wal_type_name(type), data_len,
                    replay.last_record_offset,
                    data_len,
                    replay.last_record_offset + 8 + data_len,
                    replay.last_record_offset + 8 + data_len);
                exit_code = 1;
                if (strict_crc) {
                    break;
                }
                /* Without -c, warn but continue reading */
                continue;
            }
            if (ret < 0) {
                fprintf(stderr, "error reading wal %s (ret=%d)\n", wal_path, ret);
                exit_code = 1;
                break;
            }

            if (!record_matches(sequence, type, data, data_len,
                                seq_ranges, n_seq, time_ranges, n_time)) {
                continue;
            }

            rendered++;

            /* Render record */
            fprintf(out, "seq[%" PRIu64 "] type[%s] len[%zu] ",
                    sequence, wal_type_name(type), data_len);

            /* memcpy to local to avoid unaligned access (UBSan) */
            switch (type) {
                case OM_WAL_INSERT:
                    if (data_len >= sizeof(OmWalInsert)) {
                        OmWalInsert rec_i;
                        memcpy(&rec_i, data, sizeof(rec_i));
                        print_insert(out, &rec_i, format_ts);
                    }
                    break;
                case OM_WAL_CANCEL:
                    if (data_len == sizeof(OmWalCancel)) {
                        OmWalCancel rec_c;
                        memcpy(&rec_c, data, sizeof(rec_c));
                        print_cancel(out, &rec_c, format_ts);
                    }
                    break;
                case OM_WAL_MATCH:
                    if (data_len == sizeof(OmWalMatch)) {
                        OmWalMatch rec_m;
                        memcpy(&rec_m, data, sizeof(rec_m));
                        print_match(out, &rec_m, format_ts);
                    }
                    break;
                case OM_WAL_DEACTIVATE:
                    if (data_len == sizeof(OmWalDeactivate)) {
                        OmWalDeactivate rec_d;
                        memcpy(&rec_d, data, sizeof(rec_d));
                        print_deactivate(out, &rec_d, format_ts);
                    }
                    break;
                case OM_WAL_ACTIVATE:
                    if (data_len == sizeof(OmWalActivate)) {
                        OmWalActivate rec_a;
                        memcpy(&rec_a, data, sizeof(rec_a));
                        print_activate(out, &rec_a, format_ts);
                    }
                    break;
                default:
                    if (type >= OM_WAL_USER_BASE) {
                        fprintf(out, "user[%zu]", data_len);
                    }
                    break;
            }
            fprintf(out, "\n");

            /* Replay to SHM bus */
            if (stream) {
                int rc = om_bus_stream_publish(stream, sequence, type,
                                               data, (uint16_t)data_len);
                if (rc != 0) {
                    fprintf(stderr, "publish failed seq=%" PRIu64 " (rc=%d)\n",
                            sequence, rc);
                } else {
                    replayed++;
                }
            }
        }

        om_wal_replay_close(&replay);
    }

    if (stream) {
        fprintf(stderr, "replay done: %" PRIu64 " rendered, %" PRIu64 " published to '%s'\n",
                rendered, replayed, stream_name);
        om_bus_stream_destroy(stream);
    }

    return exit_code;
}
