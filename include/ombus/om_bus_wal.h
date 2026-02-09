#ifndef OM_BUS_WAL_H
#define OM_BUS_WAL_H

/**
 * @file om_bus_wal.h
 * @brief Header-only glue: WAL post_write → bus stream publish
 *
 * Application code includes this to wire an OmWal's post_write callback
 * to an OmBusStream. libopenmatch does NOT link against libombus —
 * the connection is made via a generic function pointer on OmWal.
 *
 * Usage:
 *   om_bus_attach_wal(om_engine_get_wal(engine), stream);
 */

#include "ombus/om_bus.h"
#include "openmatch/om_wal.h"

static inline void _om_bus_wal_cb(uint64_t seq, uint8_t type,
                                   const void *data, uint16_t len, void *ctx) {
    om_bus_stream_publish((OmBusStream *)ctx, seq, type, data, len);
}

/**
 * Attach a WAL to a bus stream so every WAL write is published to the bus.
 * @param wal    WAL context (from om_engine_get_wal or standalone)
 * @param stream Bus stream (producer side)
 */
static inline void om_bus_attach_wal(OmWal *wal, OmBusStream *stream) {
    om_wal_set_post_write(wal, _om_bus_wal_cb, stream);
}

#endif /* OM_BUS_WAL_H */
