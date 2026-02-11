#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdbool.h>
#include <inttypes.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include "openmatch/om_wal.h"

typedef struct WalQueryVtab {
    sqlite3_vtab base;
    char *filename;
    char *pattern;
    OmWalConfig config;
} WalQueryVtab;

typedef struct WalQueryCursor {
    sqlite3_vtab_cursor base;
    OmWalReplay replay;
    bool replay_init;
    bool eof;
    OmWalType type;
    uint64_t sequence;
    size_t data_len;
    OmWalInsert insert;
    OmWalCancel cancel;
    OmWalMatch match;
    OmWalDeactivate deactivate;
    OmWalActivate activate;
    uint64_t user_type;
} WalQueryCursor;

enum WalQueryColumn {
    WAL_COL_SEQ = 0,
    WAL_COL_TYPE,
    WAL_COL_TYPE_NAME,
    WAL_COL_DATA_LEN,
    WAL_COL_ORDER_ID,
    WAL_COL_PRICE,
    WAL_COL_VOLUME,
    WAL_COL_VOL_REMAIN,
    WAL_COL_ORG,
    WAL_COL_FLAGS,
    WAL_COL_PRODUCT_ID,
    WAL_COL_TIMESTAMP_NS,
    WAL_COL_SLOT_IDX,
    WAL_COL_MAKER_ID,
    WAL_COL_TAKER_ID,
    WAL_COL_MATCH_PRICE,
    WAL_COL_MATCH_VOLUME,
    WAL_COL_USER_TYPE
};

static const char *wal_type_name(OmWalType type) {
    switch (type) {
        case OM_WAL_INSERT: return "INSERT";
        case OM_WAL_CANCEL: return "CANCEL";
        case OM_WAL_MATCH: return "MATCH";
        case OM_WAL_CHECKPOINT: return "CHECKPOINT";
        case OM_WAL_DEACTIVATE: return "DEACTIVATE";
        case OM_WAL_ACTIVATE: return "ACTIVATE";
        default: return (type >= OM_WAL_USER_BASE) ? "USER" : "UNKNOWN";
    }
}

static int wal_query_disconnect(sqlite3_vtab *pVtab) {
    WalQueryVtab *vtab = (WalQueryVtab *)pVtab;
    if (vtab) {
        sqlite3_free(vtab->filename);
        sqlite3_free(vtab->pattern);
        sqlite3_free(vtab);
    }
    return SQLITE_OK;
}

static int wal_query_connect(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                             sqlite3_vtab **ppVtab, char **pzErr) {
    (void)pAux;
    const char *filename = NULL;
    const char *pattern = NULL;
    uint32_t file_index = 0;
    size_t user_data_size = 0;
    size_t aux_data_size = 0;
    bool enable_crc32 = false;

    for (int i = 3; i < argc; i++) {
        const char *arg = argv[i];
        if (!arg) {
            continue;
        }
        const char *eq = strchr(arg, '=');
        if (eq) {
            size_t key_len = (size_t)(eq - arg);
            const char *val = eq + 1;
            if (key_len == 4 && strncasecmp(arg, "file", 4) == 0) {
                filename = val;
            } else if (key_len == 7 && strncasecmp(arg, "pattern", 7) == 0) {
                pattern = val;
            } else if (key_len == 5 && strncasecmp(arg, "index", 5) == 0) {
                file_index = (uint32_t)strtoul(val, NULL, 10);
            } else if (key_len == 9 && strncasecmp(arg, "user_data", 9) == 0) {
                user_data_size = (size_t)strtoull(val, NULL, 10);
            } else if (key_len == 8 && strncasecmp(arg, "aux_data", 8) == 0) {
                aux_data_size = (size_t)strtoull(val, NULL, 10);
            } else if (key_len == 5 && strncasecmp(arg, "crc32", 5) == 0) {
                enable_crc32 = (strtoul(val, NULL, 10) != 0);
            }
        } else if (!filename) {
            filename = arg;
        }
    }

    if (!filename && !pattern) {
        *pzErr = sqlite3_mprintf("wal_query requires file=PATH or a filename argument");
        return SQLITE_ERROR;
    }

    WalQueryVtab *vtab = sqlite3_malloc(sizeof(WalQueryVtab));
    if (!vtab) {
        return SQLITE_NOMEM;
    }
    memset(vtab, 0, sizeof(WalQueryVtab));

    vtab->filename = filename ? sqlite3_mprintf("%s", filename) : NULL;
    vtab->pattern = pattern ? sqlite3_mprintf("%s", pattern) : NULL;
    vtab->config.filename = vtab->filename ? vtab->filename : "";
    vtab->config.filename_pattern = vtab->pattern;
    vtab->config.file_index = file_index;
    vtab->config.user_data_size = user_data_size;
    vtab->config.aux_data_size = aux_data_size;
    vtab->config.disable_crc32 = !enable_crc32;
    vtab->config.buffer_size = 0;
    vtab->config.sync_interval_ms = 0;
    vtab->config.use_direct_io = false;
    vtab->config.wal_max_file_size = 0;

    int rc = sqlite3_declare_vtab(db,
                                 "CREATE TABLE x("
                                 "seq INTEGER,"
                                 "type INTEGER,"
                                 "type_name TEXT,"
                                 "data_len INTEGER,"
                                 "order_id INTEGER,"
                                 "price INTEGER,"
                                 "volume INTEGER,"
                                 "vol_remain INTEGER,"
                                 "org INTEGER,"
                                 "flags INTEGER,"
                                 "product_id INTEGER,"
                                 "timestamp_ns INTEGER,"
                                 "slot_idx INTEGER,"
                                 "maker_id INTEGER,"
                                 "taker_id INTEGER,"
                                 "match_price INTEGER,"
                                 "match_volume INTEGER,"
                                 "user_type INTEGER"
                                 ")");
    if (rc != SQLITE_OK) {
        wal_query_disconnect(&vtab->base);
        return rc;
    }

    *ppVtab = &vtab->base;
    return SQLITE_OK;
}

static int wal_query_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pIdxInfo) {
    (void)pVtab;
    (void)pIdxInfo;
    return SQLITE_OK;
}

static int wal_query_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
    (void)pVtab;
    WalQueryCursor *cursor = sqlite3_malloc(sizeof(WalQueryCursor));
    if (!cursor) {
        return SQLITE_NOMEM;
    }
    memset(cursor, 0, sizeof(WalQueryCursor));
    cursor->replay_init = false;
    cursor->eof = true;
    *ppCursor = &cursor->base;
    return SQLITE_OK;
}

static int wal_query_close(sqlite3_vtab_cursor *cur) {
    WalQueryCursor *cursor = (WalQueryCursor *)cur;
    if (cursor) {
        if (cursor->replay_init) {
            om_wal_replay_close(&cursor->replay);
        }
        sqlite3_free(cursor);
    }
    return SQLITE_OK;
}

static int wal_query_next(sqlite3_vtab_cursor *cur) {
    WalQueryCursor *cursor = (WalQueryCursor *)cur;
    if (!cursor->replay_init) {
        cursor->eof = true;
        return SQLITE_OK;
    }

    OmWalType type;
    void *data = NULL;
    uint64_t sequence = 0;
    size_t data_len = 0;

    int ret = om_wal_replay_next(&cursor->replay, &type, &data, &sequence, &data_len);
    if (ret == 1) {
        cursor->type = type;
        cursor->sequence = sequence;
        cursor->data_len = data_len;
        cursor->user_type = 0;
        cursor->eof = false;

        memset(&cursor->insert, 0, sizeof(cursor->insert));
        memset(&cursor->cancel, 0, sizeof(cursor->cancel));
        memset(&cursor->match, 0, sizeof(cursor->match));
        memset(&cursor->deactivate, 0, sizeof(cursor->deactivate));
        memset(&cursor->activate, 0, sizeof(cursor->activate));

        switch (type) {
            case OM_WAL_INSERT:
                if (data && data_len >= sizeof(OmWalInsert)) {
                    memcpy(&cursor->insert, data, sizeof(OmWalInsert));
                }
                break;
            case OM_WAL_CANCEL:
                if (data && data_len >= sizeof(OmWalCancel)) {
                    memcpy(&cursor->cancel, data, sizeof(OmWalCancel));
                }
                break;
            case OM_WAL_MATCH:
                if (data && data_len >= sizeof(OmWalMatch)) {
                    memcpy(&cursor->match, data, sizeof(OmWalMatch));
                }
                break;
            case OM_WAL_DEACTIVATE:
                if (data && data_len >= sizeof(OmWalDeactivate)) {
                    memcpy(&cursor->deactivate, data, sizeof(OmWalDeactivate));
                }
                break;
            case OM_WAL_ACTIVATE:
                if (data && data_len >= sizeof(OmWalActivate)) {
                    memcpy(&cursor->activate, data, sizeof(OmWalActivate));
                }
                break;
            default:
                if (type >= OM_WAL_USER_BASE) {
                    cursor->user_type = (uint64_t)type;
                }
                break;
        }
    } else if (ret == 0) {
        cursor->eof = true;
    } else {
        cursor->eof = true;
        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

static int wal_query_filter(sqlite3_vtab_cursor *cur, int idxNum, const char *idxStr,
                            int argc, sqlite3_value **argv) {
    (void)idxNum;
    (void)idxStr;
    (void)argc;
    (void)argv;

    WalQueryCursor *cursor = (WalQueryCursor *)cur;
    WalQueryVtab *vtab = (WalQueryVtab *)cur->pVtab;

    if (cursor->replay_init) {
        om_wal_replay_close(&cursor->replay);
        cursor->replay_init = false;
    }

    int rc = om_wal_replay_init_with_config(&cursor->replay, vtab->config.filename, &vtab->config);
    if (rc != 0) {
        cursor->eof = true;
        return SQLITE_ERROR;
    }

    cursor->replay_init = true;
    cursor->eof = false;
    return wal_query_next(cur);
}

static int wal_query_eof(sqlite3_vtab_cursor *cur) {
    WalQueryCursor *cursor = (WalQueryCursor *)cur;
    return cursor->eof ? 1 : 0;
}

static int wal_query_column(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col) {
    WalQueryCursor *cursor = (WalQueryCursor *)cur;

    switch (col) {
        case WAL_COL_SEQ:
            sqlite3_result_int64(ctx, (sqlite3_int64)cursor->sequence);
            break;
        case WAL_COL_TYPE:
            sqlite3_result_int(ctx, (int)cursor->type);
            break;
        case WAL_COL_TYPE_NAME:
            sqlite3_result_text(ctx, wal_type_name(cursor->type), -1, SQLITE_STATIC);
            break;
        case WAL_COL_DATA_LEN:
            sqlite3_result_int64(ctx, (sqlite3_int64)cursor->data_len);
            break;
        case WAL_COL_ORDER_ID:
            if (cursor->type == OM_WAL_INSERT) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->insert.order_id);
            } else if (cursor->type == OM_WAL_CANCEL) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->cancel.order_id);
            } else if (cursor->type == OM_WAL_DEACTIVATE) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->deactivate.order_id);
            } else if (cursor->type == OM_WAL_ACTIVATE) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->activate.order_id);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_PRICE:
            if (cursor->type == OM_WAL_INSERT) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->insert.price);
            } else if (cursor->type == OM_WAL_MATCH) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->match.price);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_VOLUME:
            if (cursor->type == OM_WAL_INSERT) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->insert.volume);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_VOL_REMAIN:
            if (cursor->type == OM_WAL_INSERT) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->insert.vol_remain);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_ORG:
            if (cursor->type == OM_WAL_INSERT) {
                sqlite3_result_int(ctx, (int)cursor->insert.org);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_FLAGS:
            if (cursor->type == OM_WAL_INSERT) {
                sqlite3_result_int(ctx, (int)cursor->insert.flags);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_PRODUCT_ID:
            if (cursor->type == OM_WAL_INSERT) {
                sqlite3_result_int(ctx, (int)cursor->insert.product_id);
            } else if (cursor->type == OM_WAL_CANCEL) {
                sqlite3_result_int(ctx, (int)cursor->cancel.product_id);
            } else if (cursor->type == OM_WAL_MATCH) {
                sqlite3_result_int(ctx, (int)cursor->match.product_id);
            } else if (cursor->type == OM_WAL_DEACTIVATE) {
                sqlite3_result_int(ctx, (int)cursor->deactivate.product_id);
            } else if (cursor->type == OM_WAL_ACTIVATE) {
                sqlite3_result_int(ctx, (int)cursor->activate.product_id);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_TIMESTAMP_NS:
            if (cursor->type == OM_WAL_INSERT) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->insert.timestamp_ns);
            } else if (cursor->type == OM_WAL_CANCEL) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->cancel.timestamp_ns);
            } else if (cursor->type == OM_WAL_MATCH) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->match.timestamp_ns);
            } else if (cursor->type == OM_WAL_DEACTIVATE) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->deactivate.timestamp_ns);
            } else if (cursor->type == OM_WAL_ACTIVATE) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->activate.timestamp_ns);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_SLOT_IDX:
            if (cursor->type == OM_WAL_CANCEL) {
                sqlite3_result_int(ctx, (int)cursor->cancel.slot_idx);
            } else if (cursor->type == OM_WAL_DEACTIVATE) {
                sqlite3_result_int(ctx, (int)cursor->deactivate.slot_idx);
            } else if (cursor->type == OM_WAL_ACTIVATE) {
                sqlite3_result_int(ctx, (int)cursor->activate.slot_idx);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_MAKER_ID:
            if (cursor->type == OM_WAL_MATCH) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->match.maker_id);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_TAKER_ID:
            if (cursor->type == OM_WAL_MATCH) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->match.taker_id);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_MATCH_PRICE:
            if (cursor->type == OM_WAL_MATCH) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->match.price);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_MATCH_VOLUME:
            if (cursor->type == OM_WAL_MATCH) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->match.volume);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        case WAL_COL_USER_TYPE:
            if (cursor->user_type != 0) {
                sqlite3_result_int64(ctx, (sqlite3_int64)cursor->user_type);
            } else {
                sqlite3_result_null(ctx);
            }
            break;
        default:
            sqlite3_result_null(ctx);
            break;
    }

    return SQLITE_OK;
}

static int wal_query_rowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *pRowid) {
    WalQueryCursor *cursor = (WalQueryCursor *)cur;
    *pRowid = (sqlite3_int64)cursor->sequence;
    return SQLITE_OK;
}

static sqlite3_module wal_query_module = {
    .iVersion = 1,
    .xCreate = wal_query_connect,
    .xConnect = wal_query_connect,
    .xBestIndex = wal_query_bestindex,
    .xDisconnect = wal_query_disconnect,
    .xDestroy = wal_query_disconnect,
    .xOpen = wal_query_open,
    .xClose = wal_query_close,
    .xFilter = wal_query_filter,
    .xNext = wal_query_next,
    .xEof = wal_query_eof,
    .xColumn = wal_query_column,
    .xRowid = wal_query_rowid,
};

int sqlite3_wal_query_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    (void)pzErrMsg;
    SQLITE_EXTENSION_INIT2(pApi);
    return sqlite3_create_module_v2(db, "wal_query", &wal_query_module, NULL, NULL);
}

int sqlite3_extension_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    return sqlite3_wal_query_init(db, pzErrMsg, pApi);
}
