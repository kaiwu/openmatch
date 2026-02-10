#ifndef OM_BUS_TCP_H
#define OM_BUS_TCP_H

/**
 * @file om_bus_tcp.h
 * @brief TCP transport for WAL record distribution across hosts
 *
 * A relay process reads from OmBusEndpoint (SHM) and broadcasts over TCP
 * to remote OmBusTcpClients. SHM stays for local same-host workers,
 * TCP for remote.
 *
 * Server: OmBusTcpServer — binds, accepts connections, broadcasts frames
 * Client: OmBusTcpClient — connects, polls frames into OmBusRecord
 */

#include <stdbool.h>
#include <stdint.h>

#include "om_bus.h"
#include "om_bus_error.h"

/* ============================================================================
 * Wire Protocol — 16-byte header + payload
 * ============================================================================ */

#define OM_BUS_TCP_FRAME_MAGIC       0x4F4D5446U  /* "OMTF" */
#define OM_BUS_TCP_FRAME_HEADER_SIZE 16U

typedef struct OmBusTcpFrameHeader {
    uint32_t magic;        /* OM_BUS_TCP_FRAME_MAGIC */
    uint8_t  wal_type;     /* OmWalType enum value */
    uint8_t  flags;        /* Reserved (0) */
    uint16_t payload_len;  /* Payload bytes (LE) */
    uint64_t wal_seq;      /* WAL sequence (LE) */
} __attribute__((packed)) OmBusTcpFrameHeader;

/* ============================================================================
 * Server API
 * ============================================================================ */

typedef struct OmBusTcpServerConfig {
    const char *bind_addr;      /* NULL = "0.0.0.0" */
    uint16_t    port;           /* 0 = ephemeral */
    uint32_t    max_clients;    /* default 64 */
    uint32_t    send_buf_size;  /* per-client, default 256 KB */
} OmBusTcpServerConfig;

typedef struct OmBusTcpServer OmBusTcpServer;

/**
 * Create a TCP server, bind and listen.
 * @param out Output server handle
 * @param cfg Server configuration
 * @return 0 on success, negative on error
 */
int om_bus_tcp_server_create(OmBusTcpServer **out, const OmBusTcpServerConfig *cfg);

/**
 * Serialize frame into each client's send buffer. Does NOT flush.
 * If a client's send buffer overflows, it is marked for disconnection.
 * @param srv      Server handle
 * @param wal_seq  WAL sequence number
 * @param wal_type OmWalType enum value
 * @param payload  Raw WAL record data
 * @param len      Payload byte count
 * @return 0 on success
 */
int om_bus_tcp_server_broadcast(OmBusTcpServer *srv, uint64_t wal_seq,
                                uint8_t wal_type, const void *payload, uint16_t len);

/**
 * Drive I/O: accept connections, flush send buffers, detect disconnects.
 * Non-blocking (poll with timeout=0).
 * @param srv Server handle
 * @return 0 on success, negative on poll error
 */
int om_bus_tcp_server_poll_io(OmBusTcpServer *srv);

/**
 * Get number of currently connected clients.
 */
uint32_t om_bus_tcp_server_client_count(const OmBusTcpServer *srv);

/**
 * Get actual bound port (useful when port=0 for ephemeral).
 */
uint16_t om_bus_tcp_server_port(const OmBusTcpServer *srv);

/**
 * Server-side statistics snapshot.
 */
typedef struct OmBusTcpServerStats {
    uint64_t records_broadcast;      /* total frames broadcast */
    uint64_t bytes_broadcast;        /* total payload bytes */
    uint64_t clients_accepted;       /* cumulative accepts */
    uint64_t clients_disconnected;   /* cumulative disconnects */
    uint64_t slow_client_drops;      /* disconnects due to buffer overflow */
} OmBusTcpServerStats;

/**
 * Snapshot current server statistics.
 */
void om_bus_tcp_server_stats(const OmBusTcpServer *srv, OmBusTcpServerStats *out);

/**
 * Destroy server, close all client connections and listen socket.
 * @param srv Server handle (NULL-safe)
 */
void om_bus_tcp_server_destroy(OmBusTcpServer *srv);

/* ============================================================================
 * Client API
 * ============================================================================ */

typedef struct OmBusTcpClientConfig {
    const char *host;           /* e.g., "127.0.0.1" */
    uint16_t    port;
    uint32_t    recv_buf_size;  /* default 256 KB */
} OmBusTcpClientConfig;

typedef struct OmBusTcpClient OmBusTcpClient;

/**
 * Connect to a TCP server (blocking connect, then sets non-blocking).
 * @param out Output client handle
 * @param cfg Client configuration
 * @return 0 on success, negative on error
 */
int om_bus_tcp_client_connect(OmBusTcpClient **out, const OmBusTcpClientConfig *cfg);

/**
 * Poll for next frame. Non-blocking.
 * @param client Client handle
 * @param rec    Output record (payload valid until next poll call)
 * @return 1 (record available), 0 (no complete frame),
 *         OM_ERR_BUS_GAP_DETECTED, OM_ERR_BUS_TCP_DISCONNECTED,
 *         OM_ERR_BUS_TCP_PROTOCOL
 */
int om_bus_tcp_client_poll(OmBusTcpClient *client, OmBusRecord *rec);

/**
 * Get last consumed WAL sequence number.
 */
uint64_t om_bus_tcp_client_wal_seq(const OmBusTcpClient *client);

/**
 * Close client connection and free resources.
 * @param client Client handle (NULL-safe)
 */
void om_bus_tcp_client_close(OmBusTcpClient *client);

#endif /* OM_BUS_TCP_H */
