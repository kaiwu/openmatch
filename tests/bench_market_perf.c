#include "openmarket/om_market.h"
#include "openmatch/om_error.h"

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct BenchConfig {
    uint32_t orgs;
    uint16_t max_products;
    uint32_t iters;
    uint32_t warmup;
    uint32_t total_orgs;
} BenchConfig;

static uint64_t bench_dealable(const OmWalInsert *rec, uint16_t viewer_org, void *ctx) {
    (void)ctx;
    if (viewer_org == rec->org) {
        return 0;
    }
    return rec->vol_remain;
}

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int parse_u32(const char *s, uint32_t *out) {
    char *end = NULL;
    unsigned long value = strtoul(s, &end, 10);
    if (!s || *s == '\0' || !end || *end != '\0') {
        return -1;
    }
    if (value > UINT32_MAX) {
        return -1;
    }
    *out = (uint32_t)value;
    return 0;
}

static void print_usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [--orgs N] [--products N] [--iters N] [--warmup N] [--total-orgs N]\n",
            prog);
}

static int parse_args(int argc, char **argv, BenchConfig *cfg) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--orgs") == 0 && i + 1 < argc) {
            if (parse_u32(argv[++i], &cfg->orgs) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--products") == 0 && i + 1 < argc) {
            uint32_t value = 0;
            if (parse_u32(argv[++i], &value) != 0 || value > UINT16_MAX) {
                return -1;
            }
            cfg->max_products = (uint16_t)value;
        } else if (strcmp(argv[i], "--iters") == 0 && i + 1 < argc) {
            if (parse_u32(argv[++i], &cfg->iters) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc) {
            if (parse_u32(argv[++i], &cfg->warmup) != 0) {
                return -1;
            }
        } else if (strcmp(argv[i], "--total-orgs") == 0 && i + 1 < argc) {
            if (parse_u32(argv[++i], &cfg->total_orgs) != 0) {
                return -1;
            }
        } else {
            return -1;
        }
    }

    if (cfg->orgs == 0 || cfg->orgs > UINT16_MAX) {
        return -1;
    }
    if (cfg->max_products == 0 || cfg->iters == 0) {
        return -1;
    }
    return 0;
}

typedef struct BenchEnv {
    OmMarket market;
    OmMarketWorker *worker;
    uint32_t *org_to_worker;
    uint32_t *product_to_public_worker;
    OmMarketSubscription *subs;
} BenchEnv;

static void bench_env_destroy(BenchEnv *env) {
    if (!env) {
        return;
    }
    om_market_destroy(&env->market);
    free(env->subs);
    free(env->org_to_worker);
    free(env->product_to_public_worker);
    memset(env, 0, sizeof(*env));
}

static int bench_env_init(BenchEnv *env, const BenchConfig *cfg, uint32_t expected_orders) {
    memset(env, 0, sizeof(*env));

    env->org_to_worker = calloc((size_t)UINT16_MAX + 1U, sizeof(*env->org_to_worker));
    env->product_to_public_worker = calloc((size_t)cfg->max_products, sizeof(*env->product_to_public_worker));
    env->subs = calloc(cfg->orgs, sizeof(*env->subs));
    if (!env->org_to_worker || !env->product_to_public_worker || !env->subs) {
        bench_env_destroy(env);
        return OM_ERR_ALLOC_FAILED;
    }

    for (uint32_t i = 0; i < cfg->orgs; i++) {
        env->subs[i].org_id = (uint16_t)(i + 1U);
        env->subs[i].product_id = 0;
    }

    OmMarketConfig mc = {
        .max_products = cfg->max_products,
        .worker_count = 1,
        .public_worker_count = 1,
        .org_to_worker = env->org_to_worker,
        .product_to_public_worker = env->product_to_public_worker,
        .subs = env->subs,
        .sub_count = cfg->orgs,
        .expected_orders_per_worker = expected_orders,
        .expected_subscribers_per_product = cfg->orgs,
        .expected_price_levels = 32,
        .top_levels = 10,
        .dealable = bench_dealable,
        .dealable_ctx = NULL,
    };

    int ret = om_market_init(&env->market, &mc);
    if (ret != 0) {
        bench_env_destroy(env);
        return ret;
    }

    env->worker = om_market_worker(&env->market, 0);
    if (!env->worker) {
        bench_env_destroy(env);
        return OM_ERR_ALLOC_FAILED;
    }

    return 0;
}

static int bench_insert_ns(const BenchConfig *cfg, double *out_ns) {
    BenchEnv env;
    int ret = bench_env_init(&env, cfg, cfg->iters + cfg->warmup + 16U);
    if (ret != 0) {
        return ret;
    }

    uint32_t total = cfg->warmup + cfg->iters;
    uint64_t t0 = 0;
    for (uint32_t i = 0; i < total; i++) {
        OmWalInsert ins = {
            .order_id = (uint64_t)i + 1ULL,
            .price = 1000 + (i % 64U),
            .volume = 100,
            .vol_remain = 100,
            .org = 1,
            .flags = OM_SIDE_BID,
            .product_id = 0,
        };
        if (i == cfg->warmup) {
            t0 = now_ns();
        }
        ret = om_market_worker_process(env.worker, OM_WAL_INSERT, &ins);
        if (ret != 0) {
            bench_env_destroy(&env);
            return ret;
        }
    }
    uint64_t t1 = now_ns();
    *out_ns = (double)(t1 - t0) / (double)cfg->iters;
    bench_env_destroy(&env);
    return 0;
}

static int preload_orders(BenchEnv *env, uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        OmWalInsert ins = {
            .order_id = (uint64_t)i + 1ULL,
            .price = 1000 + (i % 64U),
            .volume = 100,
            .vol_remain = 100,
            .org = 1,
            .flags = OM_SIDE_BID,
            .product_id = 0,
        };
        int ret = om_market_worker_process(env->worker, OM_WAL_INSERT, &ins);
        if (ret != 0) {
            return ret;
        }
    }
    return 0;
}

static int bench_match_ns(const BenchConfig *cfg, double *out_ns) {
    BenchEnv env;
    int ret = bench_env_init(&env, cfg, cfg->iters + cfg->warmup + 16U);
    if (ret != 0) {
        return ret;
    }

    uint32_t total = cfg->warmup + cfg->iters;
    ret = preload_orders(&env, total + 8U);
    if (ret != 0) {
        bench_env_destroy(&env);
        return ret;
    }

    uint64_t t0 = 0;
    for (uint32_t i = 0; i < total; i++) {
        OmWalMatch m = {
            .maker_id = (uint64_t)i + 1ULL,
            .taker_id = 9000000ULL + (uint64_t)i,
            .price = 1000 + (i % 64U),
            .volume = 10,
            .product_id = 0,
        };
        if (i == cfg->warmup) {
            t0 = now_ns();
        }
        ret = om_market_worker_process(env.worker, OM_WAL_MATCH, &m);
        if (ret != 0) {
            bench_env_destroy(&env);
            return ret;
        }
    }
    uint64_t t1 = now_ns();
    *out_ns = (double)(t1 - t0) / (double)cfg->iters;
    bench_env_destroy(&env);
    return 0;
}

static int bench_cancel_ns(const BenchConfig *cfg, double *out_ns) {
    BenchEnv env;
    int ret = bench_env_init(&env, cfg, cfg->iters + cfg->warmup + 16U);
    if (ret != 0) {
        return ret;
    }

    uint32_t total = cfg->warmup + cfg->iters;
    ret = preload_orders(&env, total + 8U);
    if (ret != 0) {
        bench_env_destroy(&env);
        return ret;
    }

    uint64_t t0 = 0;
    for (uint32_t i = 0; i < total; i++) {
        OmWalCancel c = {
            .order_id = (uint64_t)i + 1ULL,
            .product_id = 0,
        };
        if (i == cfg->warmup) {
            t0 = now_ns();
        }
        ret = om_market_worker_process(env.worker, OM_WAL_CANCEL, &c);
        if (ret != 0) {
            bench_env_destroy(&env);
            return ret;
        }
    }
    uint64_t t1 = now_ns();
    *out_ns = (double)(t1 - t0) / (double)cfg->iters;
    bench_env_destroy(&env);
    return 0;
}

static int run_profile(const BenchConfig *cfg,
                       uint32_t profile_orgs,
                       double *insert_ns,
                       double *match_ns,
                       double *cancel_ns,
                       double *blended_ns) {
    BenchConfig local = *cfg;
    local.orgs = profile_orgs;

    int ret = bench_insert_ns(&local, insert_ns);
    if (ret != 0) {
        return ret;
    }
    ret = bench_match_ns(&local, match_ns);
    if (ret != 0) {
        return ret;
    }
    ret = bench_cancel_ns(&local, cancel_ns);
    if (ret != 0) {
        return ret;
    }

    *blended_ns = *insert_ns * 0.6 + *match_ns * 0.3 + *cancel_ns * 0.1;
    return 0;
}

int main(int argc, char **argv) {
    BenchConfig cfg = {
        .orgs = 1024,
        .max_products = 10000,
        .iters = 20000,
        .warmup = 2000,
        .total_orgs = 5000,
    };

    if (parse_args(argc, argv, &cfg) != 0) {
        print_usage(argv[0]);
        return 2;
    }

    uint32_t low_orgs = cfg.orgs >= 128U ? 128U : (cfg.orgs > 16U ? cfg.orgs / 2U : cfg.orgs);
    if (low_orgs == 0) {
        low_orgs = 1;
    }

    double i_low = 0.0, m_low = 0.0, c_low = 0.0, b_low = 0.0;
    double i_high = 0.0, m_high = 0.0, c_high = 0.0, b_high = 0.0;

    int ret = run_profile(&cfg, low_orgs, &i_low, &m_low, &c_low, &b_low);
    if (ret != 0) {
        fprintf(stderr, "profile(low=%u) failed: %d\n", low_orgs, ret);
        return 1;
    }

    ret = run_profile(&cfg, cfg.orgs, &i_high, &m_high, &c_high, &b_high);
    if (ret != 0) {
        fprintf(stderr, "profile(high=%u) failed: %d\n", cfg.orgs, ret);
        return 1;
    }

    double per_org_ns = 0.0;
    double fixed_ns = b_high;
    if (cfg.orgs > low_orgs) {
        per_org_ns = (b_high - b_low) / (double)(cfg.orgs - low_orgs);
        fixed_ns = b_high - per_org_ns * (double)cfg.orgs;
    }
    if (per_org_ns < 0.0) {
        per_org_ns = 0.0;
    }
    if (fixed_ns < 0.0) {
        fixed_ns = 0.0;
    }

    double denom = 1000.0 - fixed_ns;
    double required_workers = 0.0;
    if (denom > 0.0) {
        required_workers = ceil(((double)cfg.total_orgs * per_org_ns) / denom);
    }

    printf("OpenMarket private-worker perf harness\n");
    printf("config: orgs(high)=%u orgs(low)=%u products=%u iters=%u warmup=%u total_orgs=%u\n",
           cfg.orgs,
           low_orgs,
           cfg.max_products,
           cfg.iters,
           cfg.warmup,
           cfg.total_orgs);

    printf("\n");
    printf("profile low (%u orgs):   insert=%.2fns match=%.2fns cancel=%.2fns blended=%.2fns\n",
           low_orgs,
           i_low,
           m_low,
           c_low,
           b_low);
    printf("profile high (%u orgs):  insert=%.2fns match=%.2fns cancel=%.2fns blended=%.2fns\n",
           cfg.orgs,
           i_high,
           m_high,
           c_high,
           b_high);

    printf("\n");
    printf("fit: fixed_ns=%.2f per_org_ns=%.4f\n", fixed_ns, per_org_ns);
    if (denom <= 0.0) {
        printf("worker_estimate: unavailable (fixed_ns >= 1000ns budget)\n");
    } else {
        printf("worker_estimate(total_orgs=%u): %.0f private workers\n",
               cfg.total_orgs,
               required_workers);
    }

    printf("formula: W >= (O * per_org_ns) / (1000 - fixed_ns)\n");
    return 0;
}
