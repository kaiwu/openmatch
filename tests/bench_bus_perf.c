#include "ombus/om_bus.h"
#include "ombus/om_bus_tcp.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct BenchCfg {
    uint32_t shm_iters;
    uint32_t tcp_iters;
    uint32_t shm_batch;
    int run_shm;
    int run_shm_mixed;
    int run_tcp;
} BenchCfg;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int parse_u32(const char *s, uint32_t *out) {
    char *end = NULL;
    unsigned long v = strtoul(s, &end, 10);
    if (!s || *s == '\0' || !end || *end != '\0' || v > UINT32_MAX) {
        return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

static int parse_args(int argc, char **argv, BenchCfg *cfg) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--shm-iters") == 0 && i + 1 < argc) {
            if (parse_u32(argv[++i], &cfg->shm_iters) != 0) return -1;
        } else if (strcmp(argv[i], "--tcp-iters") == 0 && i + 1 < argc) {
            if (parse_u32(argv[++i], &cfg->tcp_iters) != 0) return -1;
        } else if (strcmp(argv[i], "--shm-batch") == 0 && i + 1 < argc) {
            if (parse_u32(argv[++i], &cfg->shm_batch) != 0) return -1;
            if (cfg->shm_batch == 0) return -1;
        } else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            const char *m = argv[++i];
            if (strcmp(m, "shm") == 0) {
                cfg->run_shm = 1;
                cfg->run_shm_mixed = 0;
                cfg->run_tcp = 0;
            } else if (strcmp(m, "shm-mixed") == 0) {
                cfg->run_shm = 0;
                cfg->run_shm_mixed = 1;
                cfg->run_tcp = 0;
            } else if (strcmp(m, "tcp") == 0) {
                cfg->run_shm = 0;
                cfg->run_shm_mixed = 0;
                cfg->run_tcp = 1;
            } else if (strcmp(m, "both") == 0) {
                cfg->run_shm = 1;
                cfg->run_shm_mixed = 1;
                cfg->run_tcp = 1;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }
    return 0;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--mode shm|shm-mixed|tcp|both] [--shm-iters N] [--shm-batch N] [--tcp-iters N]\n",
            prog);
}

static int run_shm_bench(uint32_t iters, double *ns_per_rec) {
    OmBusStream *stream = NULL;
    OmBusEndpoint *ep = NULL;

    OmBusStreamConfig scfg = {
        .stream_name = "/om-bus-bench-shm",
        .capacity = 4096,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
        .staleness_ns = 0,
        .backpressure_cb = NULL,
        .backpressure_ctx = NULL,
    };
    int rc = om_bus_stream_create(&stream, &scfg);
    if (rc != 0) return rc;

    OmBusEndpointConfig ecfg = {
        .stream_name = "/om-bus-bench-shm",
        .consumer_index = 0,
        .zero_copy = true,
    };
    rc = om_bus_endpoint_open(&ep, &ecfg);
    if (rc != 0) {
        om_bus_stream_destroy(stream);
        return rc;
    }

    uint64_t t0 = now_ns();
    for (uint32_t i = 0; i < iters; i++) {
        uint64_t payload = (uint64_t)i;
        rc = om_bus_stream_publish(stream, (uint64_t)i + 1U, 1, &payload, sizeof(payload));
        if (rc != 0) {
            om_bus_endpoint_close(ep);
            om_bus_stream_destroy(stream);
            return rc;
        }
        OmBusRecord rec;
        do {
            rc = om_bus_endpoint_poll(ep, &rec);
        } while (rc == 0);
        if (rc < 0) {
            om_bus_endpoint_close(ep);
            om_bus_stream_destroy(stream);
            return rc;
        }
    }
    uint64_t t1 = now_ns();

    *ns_per_rec = (double)(t1 - t0) / (double)iters;
    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
    return 0;
}

static int run_tcp_bench(uint32_t iters, double *ns_per_rec) {
    OmBusTcpServer *srv = NULL;
    OmBusTcpClient *client = NULL;

    OmBusTcpServerConfig scfg = {
        .bind_addr = "127.0.0.1",
        .port = 0,
        .max_clients = 8,
        .send_buf_size = 256 * 1024,
    };
    int rc = om_bus_tcp_server_create(&srv, &scfg);
    if (rc != 0) return rc;

    OmBusTcpClientConfig ccfg = {
        .host = "127.0.0.1",
        .port = om_bus_tcp_server_port(srv),
        .recv_buf_size = 256 * 1024,
        .flags = 0,
    };
    rc = om_bus_tcp_client_connect(&client, &ccfg);
    if (rc != 0) {
        om_bus_tcp_server_destroy(srv);
        return rc;
    }

    for (int spin = 0; spin < 200; spin++) {
        om_bus_tcp_server_poll_io(srv);
        if (om_bus_tcp_server_client_count(srv) > 0) break;
        usleep(1000);
    }
    if (om_bus_tcp_server_client_count(srv) == 0) {
        om_bus_tcp_client_close(client);
        om_bus_tcp_server_destroy(srv);
        return OM_ERR_BUS_TCP_CONNECT;
    }

    uint64_t t0 = now_ns();
    for (uint32_t i = 0; i < iters; i++) {
        uint64_t payload = (uint64_t)i;
        rc = om_bus_tcp_server_broadcast(srv, (uint64_t)i + 1U, 1, &payload, sizeof(payload));
        if (rc != 0) {
            om_bus_tcp_client_close(client);
            om_bus_tcp_server_destroy(srv);
            return rc;
        }
        om_bus_tcp_server_poll_io(srv);

        OmBusRecord rec;
        do {
            rc = om_bus_tcp_client_poll(client, &rec);
            if (rc == 0) {
                om_bus_tcp_server_poll_io(srv);
            }
        } while (rc == 0);

        if (rc < 0 && rc != OM_ERR_BUS_GAP_DETECTED) {
            om_bus_tcp_client_close(client);
            om_bus_tcp_server_destroy(srv);
            return rc;
        }
    }
    uint64_t t1 = now_ns();

    *ns_per_rec = (double)(t1 - t0) / (double)iters;
    om_bus_tcp_client_close(client);
    om_bus_tcp_server_destroy(srv);
    return 0;
}

static int run_shm_mixed_bench(uint32_t iters,
                               uint32_t batch,
                               double *ns_per_rec) {
    OmBusStream *stream = NULL;
    OmBusEndpoint *ep = NULL;

    OmBusStreamConfig scfg = {
        .stream_name = "/om-bus-bench-shm-mixed",
        .capacity = 4096,
        .slot_size = 256,
        .max_consumers = 1,
        .flags = 0,
        .staleness_ns = 0,
        .backpressure_cb = NULL,
        .backpressure_ctx = NULL,
    };
    int rc = om_bus_stream_create(&stream, &scfg);
    if (rc != 0) return rc;

    OmBusEndpointConfig ecfg = {
        .stream_name = "/om-bus-bench-shm-mixed",
        .consumer_index = 0,
        .zero_copy = true,
    };
    rc = om_bus_endpoint_open(&ep, &ecfg);
    if (rc != 0) {
        om_bus_stream_destroy(stream);
        return rc;
    }

    OmBusRecord *pub = calloc(batch, sizeof(*pub));
    OmBusRecord *out = calloc(batch, sizeof(*out));
    uint64_t *payloads = calloc(batch, sizeof(*payloads));
    if (!pub || !out || !payloads) {
        free(pub);
        free(out);
        free(payloads);
        om_bus_endpoint_close(ep);
        om_bus_stream_destroy(stream);
        return OM_ERR_BUS_INIT;
    }

    uint64_t seq = 1;
    uint64_t t0 = now_ns();
    uint32_t done = 0;
    while (done < iters) {
        uint32_t chunk = iters - done;
        if (chunk > batch) chunk = batch;

        for (uint32_t i = 0; i < chunk; i++) {
            payloads[i] = seq;
            pub[i].wal_seq = seq;
            pub[i].wal_type = 1;
            pub[i].payload = &payloads[i];
            pub[i].payload_len = sizeof(uint64_t);
            seq++;
        }

        rc = om_bus_stream_publish_batch(stream, pub, chunk);
        if (rc != 0) {
            free(pub);
            free(out);
            free(payloads);
            om_bus_endpoint_close(ep);
            om_bus_stream_destroy(stream);
            return rc;
        }

        uint32_t consumed = 0;
        while (consumed < chunk) {
            rc = om_bus_endpoint_poll_batch(ep, out + consumed, chunk - consumed);
            if (rc < 0) {
                free(pub);
                free(out);
                free(payloads);
                om_bus_endpoint_close(ep);
                om_bus_stream_destroy(stream);
                return rc;
            }
            if (rc == 0) {
                continue;
            }
            consumed += (uint32_t)rc;
        }

        done += chunk;
    }
    uint64_t t1 = now_ns();

    free(pub);
    free(out);
    free(payloads);
    *ns_per_rec = (double)(t1 - t0) / (double)iters;
    om_bus_endpoint_close(ep);
    om_bus_stream_destroy(stream);
    return 0;
}

int main(int argc, char **argv) {
    BenchCfg cfg = {
        .shm_iters = 100000,
        .tcp_iters = 20000,
        .shm_batch = 32,
        .run_shm = 1,
        .run_shm_mixed = 1,
        .run_tcp = 1,
    };

    if (parse_args(argc, argv, &cfg) != 0) {
        usage(argv[0]);
        return 2;
    }

    printf("Message bus benchmark\n");

    if (cfg.run_shm) {
        double ns = 0.0;
        int rc = run_shm_bench(cfg.shm_iters, &ns);
        if (rc != 0) {
            fprintf(stderr, "SHM bench failed: %d\n", rc);
            return 1;
        }
        printf("SHM: iters=%u ns/rec=%.2f rec/s=%.0f\n",
               cfg.shm_iters, ns, 1e9 / ns);
    }

    if (cfg.run_shm_mixed) {
        double ns = 0.0;
        int rc = run_shm_mixed_bench(cfg.shm_iters, cfg.shm_batch, &ns);
        if (rc != 0) {
            fprintf(stderr, "SHM mixed bench failed: %d\n", rc);
            return 1;
        }
        printf("SHM(mixed,batch=%u): iters=%u ns/rec=%.2f rec/s=%.0f\n",
               cfg.shm_batch, cfg.shm_iters, ns, 1e9 / ns);
    }

    if (cfg.run_tcp) {
        double ns = 0.0;
        int rc = run_tcp_bench(cfg.tcp_iters, &ns);
        if (rc != 0) {
            fprintf(stderr, "TCP bench failed: %d\n", rc);
            return 1;
        }
        printf("TCP(loopback): iters=%u ns/rec=%.2f rec/s=%.0f\n",
               cfg.tcp_iters, ns, 1e9 / ns);
    }

    return 0;
}
