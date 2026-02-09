/**
 * @file om_bus_tcp.c
 * @brief TCP transport for WAL record distribution across hosts
 */

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "ombus/om_bus_tcp.h"

/* ============================================================================
 * Platform portability
 * ============================================================================ */

#ifdef __APPLE__
#define OM_MSG_NOSIGNAL 0
#else
#define OM_MSG_NOSIGNAL MSG_NOSIGNAL
#endif

#define OM_TCP_DEFAULT_MAX_CLIENTS   64U
#define OM_TCP_DEFAULT_SEND_BUF_SIZE (256U * 1024U)
#define OM_TCP_DEFAULT_RECV_BUF_SIZE (256U * 1024U)

/* ============================================================================
 * Internal structures
 * ============================================================================ */

typedef struct OmBusTcpClientSlot {
    int      fd;                /* -1 = unused */
    uint8_t *send_buf;
    uint32_t send_buf_size;
    uint32_t send_used;         /* total bytes pending (offset + unsent) */
    uint32_t send_offset;       /* bytes already flushed */
    bool     disconnect_pending;
} OmBusTcpClientSlot;

struct OmBusTcpServer {
    int                  listen_fd;
    struct pollfd       *pollfds;      /* [0]=listen, [1..max]=clients */
    uint32_t            *pfd_to_slot;  /* pollfds[i+1] -> client slot index */
    OmBusTcpClientSlot  *clients;
    uint32_t             max_clients;
    uint32_t             client_count;
    uint32_t             send_buf_size;
    uint16_t             port;         /* actual bound port */
};

struct OmBusTcpClient {
    int      fd;
    uint8_t *recv_buf;
    uint32_t recv_buf_size;
    uint32_t recv_used;         /* total bytes in buffer (offset + unparsed) */
    uint32_t recv_offset;       /* bytes already consumed */
    uint64_t expected_wal_seq;
    uint64_t last_wal_seq;
};

/* ============================================================================
 * Helpers
 * ============================================================================ */

static int _set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int _set_tcp_nodelay(int fd) {
    int val = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
}

#ifdef __APPLE__
static int _set_nosigpipe(int fd) {
    int val = 1;
    return setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &val, sizeof(val));
}
#endif

/* ============================================================================
 * Server
 * ============================================================================ */

int om_bus_tcp_server_create(OmBusTcpServer **out, const OmBusTcpServerConfig *cfg) {
    if (!out || !cfg) return OM_ERR_BUS_INIT;

    uint32_t max_clients = cfg->max_clients ? cfg->max_clients : OM_TCP_DEFAULT_MAX_CLIENTS;
    uint32_t send_buf_sz = cfg->send_buf_size ? cfg->send_buf_size : OM_TCP_DEFAULT_SEND_BUF_SIZE;

    OmBusTcpServer *srv = calloc(1, sizeof(*srv));
    if (!srv) return OM_ERR_BUS_INIT;

    srv->max_clients = max_clients;
    srv->send_buf_size = send_buf_sz;
    srv->client_count = 0;
    srv->listen_fd = -1;

    /* Allocate client slots */
    srv->clients = calloc(max_clients, sizeof(OmBusTcpClientSlot));
    if (!srv->clients) { free(srv); return OM_ERR_BUS_INIT; }
    for (uint32_t i = 0; i < max_clients; i++)
        srv->clients[i].fd = -1;

    /* Allocate pollfds: 1 for listen + max_clients */
    srv->pollfds = calloc(1 + max_clients, sizeof(struct pollfd));
    if (!srv->pollfds) { free(srv->clients); free(srv); return OM_ERR_BUS_INIT; }
    srv->pfd_to_slot = calloc(max_clients, sizeof(uint32_t));
    if (!srv->pfd_to_slot) { free(srv->pollfds); free(srv->clients); free(srv); return OM_ERR_BUS_INIT; }

    /* Create listen socket */
    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0) {
        free(srv->pollfds); free(srv->clients); free(srv);
        return OM_ERR_BUS_TCP_BIND;
    }

    int reuseaddr = 1;
    setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuseaddr, sizeof(reuseaddr));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg->port);
    if (cfg->bind_addr)
        inet_pton(AF_INET, cfg->bind_addr, &addr.sin_addr);
    else
        addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(srv->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(srv->listen_fd);
        free(srv->pollfds); free(srv->clients); free(srv);
        return OM_ERR_BUS_TCP_BIND;
    }

    if (listen(srv->listen_fd, 16) < 0) {
        close(srv->listen_fd);
        free(srv->pollfds); free(srv->clients); free(srv);
        return OM_ERR_BUS_TCP_BIND;
    }

    /* Retrieve actual port */
    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    getsockname(srv->listen_fd, (struct sockaddr *)&bound, &blen);
    srv->port = ntohs(bound.sin_port);

    if (_set_nonblocking(srv->listen_fd) < 0) {
        close(srv->listen_fd);
        free(srv->pollfds); free(srv->clients); free(srv);
        return OM_ERR_BUS_TCP_BIND;
    }

    *out = srv;
    return 0;
}

uint16_t om_bus_tcp_server_port(const OmBusTcpServer *srv) {
    return srv ? srv->port : 0;
}

uint32_t om_bus_tcp_server_client_count(const OmBusTcpServer *srv) {
    return srv ? srv->client_count : 0;
}

static void _server_close_client(OmBusTcpServer *srv, uint32_t idx) {
    OmBusTcpClientSlot *slot = &srv->clients[idx];
    if (slot->fd >= 0) {
        close(slot->fd);
        slot->fd = -1;
    }
    free(slot->send_buf);
    slot->send_buf = NULL;
    slot->send_used = 0;
    slot->send_offset = 0;
    slot->disconnect_pending = false;
    srv->client_count--;
}

int om_bus_tcp_server_broadcast(OmBusTcpServer *srv, uint64_t wal_seq,
                                 uint8_t wal_type, const void *payload, uint16_t len) {
    if (!srv) return OM_ERR_BUS_INIT;

    OmBusTcpFrameHeader hdr;
    hdr.magic = OM_BUS_TCP_FRAME_MAGIC;
    hdr.wal_type = wal_type;
    hdr.flags = 0;
    hdr.payload_len = len;
    hdr.wal_seq = wal_seq;

    uint32_t frame_size = OM_BUS_TCP_FRAME_HEADER_SIZE + len;

    for (uint32_t i = 0; i < srv->max_clients; i++) {
        OmBusTcpClientSlot *slot = &srv->clients[i];
        if (slot->fd < 0 || slot->disconnect_pending) continue;

        uint32_t pending = slot->send_used - slot->send_offset;

        /* Check if frame fits */
        if (pending + frame_size > slot->send_buf_size) {
            /* Compact: move unsent data to front */
            if (slot->send_offset > 0 && pending > 0) {
                memmove(slot->send_buf, slot->send_buf + slot->send_offset, pending);
            }
            slot->send_used = pending;
            slot->send_offset = 0;
        }

        /* Check again after compaction */
        if (slot->send_used + frame_size > slot->send_buf_size) {
            slot->disconnect_pending = true;
            continue;
        }

        /* Append frame */
        memcpy(slot->send_buf + slot->send_used, &hdr, OM_BUS_TCP_FRAME_HEADER_SIZE);
        if (len > 0 && payload)
            memcpy(slot->send_buf + slot->send_used + OM_BUS_TCP_FRAME_HEADER_SIZE, payload, len);
        slot->send_used += frame_size;
    }

    return 0;
}

int om_bus_tcp_server_poll_io(OmBusTcpServer *srv) {
    if (!srv) return OM_ERR_BUS_INIT;

    /* Build pollfd array */
    nfds_t nfds = 0;

    /* Listen fd */
    srv->pollfds[nfds].fd = srv->listen_fd;
    srv->pollfds[nfds].events = POLLIN;
    srv->pollfds[nfds].revents = 0;
    nfds++;

    /* Build index mapping: pollfds[nfds] -> client slot index */
    for (uint32_t i = 0; i < srv->max_clients; i++) {
        OmBusTcpClientSlot *slot = &srv->clients[i];
        if (slot->fd < 0) continue;

        uint32_t pending = slot->send_used - slot->send_offset;
        srv->pfd_to_slot[nfds - 1] = i;
        srv->pollfds[nfds].fd = slot->fd;
        srv->pollfds[nfds].events = POLLIN; /* detect disconnect */
        if (pending > 0)
            srv->pollfds[nfds].events |= POLLOUT;
        srv->pollfds[nfds].revents = 0;
        nfds++;
    }

    int ret = poll(srv->pollfds, nfds, 0);
    if (ret < 0) {
        if (errno == EINTR) return 0;
        return OM_ERR_BUS_TCP_IO;
    }
    if (ret == 0) goto cleanup;

    /* Accept new connections */
    if (srv->pollfds[0].revents & POLLIN) {
        for (;;) {
            int cfd = accept(srv->listen_fd, NULL, NULL);
            if (cfd < 0) break;

            /* Find free slot */
            uint32_t slot_idx = UINT32_MAX;
            for (uint32_t i = 0; i < srv->max_clients; i++) {
                if (srv->clients[i].fd < 0) { slot_idx = i; break; }
            }

            if (slot_idx == UINT32_MAX) {
                close(cfd); /* no room */
                continue;
            }

            _set_nonblocking(cfd);
            _set_tcp_nodelay(cfd);
#ifdef __APPLE__
            _set_nosigpipe(cfd);
#endif

            OmBusTcpClientSlot *slot = &srv->clients[slot_idx];
            slot->fd = cfd;
            slot->send_buf = malloc(srv->send_buf_size);
            slot->send_buf_size = srv->send_buf_size;
            slot->send_used = 0;
            slot->send_offset = 0;
            slot->disconnect_pending = false;
            srv->client_count++;
        }
    }

    /* Process client I/O */
    for (nfds_t p = 1; p < nfds; p++) {
        uint32_t si = srv->pfd_to_slot[p - 1];
        OmBusTcpClientSlot *slot = &srv->clients[si];
        if (slot->fd < 0) continue;

        short rev = srv->pollfds[p].revents;

        /* Detect disconnect */
        if (rev & (POLLHUP | POLLERR)) {
            slot->disconnect_pending = true;
        } else if (rev & POLLIN) {
            /* Peek to detect FIN */
            char tmp;
            ssize_t n = recv(slot->fd, &tmp, 1, MSG_PEEK | OM_MSG_NOSIGNAL);
            if (n == 0) {
                slot->disconnect_pending = true;
            }
        }

        /* Flush send buffer */
        if ((rev & POLLOUT) && !slot->disconnect_pending) {
            uint32_t pending = slot->send_used - slot->send_offset;
            if (pending > 0) {
                ssize_t n = send(slot->fd,
                                 slot->send_buf + slot->send_offset,
                                 pending, OM_MSG_NOSIGNAL);
                if (n > 0) {
                    slot->send_offset += (uint32_t)n;
                    /* If fully flushed, reset */
                    if (slot->send_offset == slot->send_used) {
                        slot->send_offset = 0;
                        slot->send_used = 0;
                    }
                } else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    slot->disconnect_pending = true;
                }
            }
        }
    }

cleanup:
    /* Close clients marked for disconnect */
    for (uint32_t i = 0; i < srv->max_clients; i++) {
        if (srv->clients[i].disconnect_pending)
            _server_close_client(srv, i);
    }

    return 0;
}

void om_bus_tcp_server_destroy(OmBusTcpServer *srv) {
    if (!srv) return;

    for (uint32_t i = 0; i < srv->max_clients; i++) {
        if (srv->clients[i].fd >= 0)
            _server_close_client(srv, i);
    }

    if (srv->listen_fd >= 0)
        close(srv->listen_fd);

    free(srv->pfd_to_slot);
    free(srv->pollfds);
    free(srv->clients);
    free(srv);
}

/* ============================================================================
 * Client
 * ============================================================================ */

int om_bus_tcp_client_connect(OmBusTcpClient **out, const OmBusTcpClientConfig *cfg) {
    if (!out || !cfg || !cfg->host) return OM_ERR_BUS_INIT;

    uint32_t recv_buf_sz = cfg->recv_buf_size ? cfg->recv_buf_size : OM_TCP_DEFAULT_RECV_BUF_SIZE;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return OM_ERR_BUS_TCP_CONNECT;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg->port);
    if (inet_pton(AF_INET, cfg->host, &addr.sin_addr) != 1) {
        close(fd);
        return OM_ERR_BUS_TCP_CONNECT;
    }

    /* Blocking connect */
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return OM_ERR_BUS_TCP_CONNECT;
    }

    /* Set non-blocking after connect */
    _set_nonblocking(fd);
    _set_tcp_nodelay(fd);
#ifdef __APPLE__
    _set_nosigpipe(fd);
#endif

    OmBusTcpClient *client = calloc(1, sizeof(*client));
    if (!client) { close(fd); return OM_ERR_BUS_INIT; }

    client->fd = fd;
    client->recv_buf = malloc(recv_buf_sz);
    if (!client->recv_buf) { close(fd); free(client); return OM_ERR_BUS_INIT; }
    client->recv_buf_size = recv_buf_sz;
    client->recv_used = 0;
    client->recv_offset = 0;
    client->expected_wal_seq = 0;
    client->last_wal_seq = 0;

    *out = client;
    return 0;
}

int om_bus_tcp_client_poll(OmBusTcpClient *client, OmBusRecord *rec) {
    if (!client || !rec) return OM_ERR_BUS_INIT;
    if (client->fd < 0) return OM_ERR_BUS_TCP_DISCONNECTED;

    /* Compact buffer if needed: move unconsumed data to front */
    if (client->recv_offset > 0) {
        uint32_t pending = client->recv_used - client->recv_offset;
        if (pending > 0)
            memmove(client->recv_buf, client->recv_buf + client->recv_offset, pending);
        client->recv_used = pending;
        client->recv_offset = 0;
    }

    /* Try to recv more data */
    if (client->recv_used < client->recv_buf_size) {
        ssize_t n = recv(client->fd,
                         client->recv_buf + client->recv_used,
                         client->recv_buf_size - client->recv_used,
                         OM_MSG_NOSIGNAL);
        if (n > 0) {
            client->recv_used += (uint32_t)n;
        } else if (n == 0) {
            return OM_ERR_BUS_TCP_DISCONNECTED;
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return OM_ERR_BUS_TCP_DISCONNECTED;
        }
    }

    /* Try to parse one frame from recv_offset */
    uint32_t avail = client->recv_used - client->recv_offset;
    if (avail < OM_BUS_TCP_FRAME_HEADER_SIZE)
        return 0;

    uint8_t *frame_start = client->recv_buf + client->recv_offset;
    OmBusTcpFrameHeader hdr;
    memcpy(&hdr, frame_start, OM_BUS_TCP_FRAME_HEADER_SIZE);

    if (hdr.magic != OM_BUS_TCP_FRAME_MAGIC)
        return OM_ERR_BUS_TCP_PROTOCOL;

    uint32_t frame_size = OM_BUS_TCP_FRAME_HEADER_SIZE + hdr.payload_len;
    if (avail < frame_size)
        return 0;

    /* Fill output record — payload points into recv buffer (stable until next poll) */
    rec->wal_seq = hdr.wal_seq;
    rec->wal_type = hdr.wal_type;
    rec->payload_len = hdr.payload_len;
    rec->payload = frame_start + OM_BUS_TCP_FRAME_HEADER_SIZE;

    /* Advance offset past this frame (data stays in buffer until next poll) */
    client->recv_offset += frame_size;

    /* Gap detection */
    int result = 1;
    if (client->expected_wal_seq > 0 && hdr.wal_seq != client->expected_wal_seq) {
        result = OM_ERR_BUS_GAP_DETECTED;
    }
    client->expected_wal_seq = hdr.wal_seq + 1;
    client->last_wal_seq = hdr.wal_seq;

    return result;
}

uint64_t om_bus_tcp_client_wal_seq(const OmBusTcpClient *client) {
    return client ? client->last_wal_seq : 0;
}

void om_bus_tcp_client_close(OmBusTcpClient *client) {
    if (!client) return;
    if (client->fd >= 0)
        close(client->fd);
    free(client->recv_buf);
    free(client);
}
