# llm-usage-gateway

[![ci](https://github.com/pratham7711/llm-usage-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/pratham7711/llm-usage-gateway/actions/workflows/ci.yml)

An OpenAI-compatible gateway that meters LLM usage per tenant. It authenticates each tenant,
enforces a rate limit and a monthly token quota, forwards the call upstream, relays the reply
(buffered or SSE streamed), and publishes one usage event per forwarded request to Kafka. A
metering service turns those events into billing rows in Postgres with no loss and no double
billing, even when its consumer is killed or the broker restarts mid-run.

Java 21 (virtual threads), Spring Boot 4, Kafka, PostgreSQL, Redis, OpenTelemetry, Prometheus,
Grafana, k6, Testcontainers, Docker Compose, Kubernetes (k3s), Terraform.

## Results

Measured on one laptop (Apple M5): a Docker VM with 10 CPUs and 7.7 GB, shared by k6, the mock
upstream and the whole stack. The mock answers in about 40 ms at the median and 90 ms at p99 (log
normal, like a fast model), so every latency below includes that. Every run reconciles k6's
successful responses against Postgres row by row. Raw data: [`results/summary.json`](results/summary.json).

| | Measured |
|---|---|
| Sustained with zero errors, gateway on 2 CPU / 1 GB | **3,000 req/s**, p50 40.9 ms, p99 90.6 ms. The mock alone gives p50 40.2 / p99 90.2 ms at that rate, so the gateway's own overhead is below what this setup can resolve. 3,000 was the top of the step ladder, not a failure point. |
| Peak served, 2 CPUs | **5,400 req/s** at 6,000 offered; the other 10% got an immediate 503, admitted p99 99 ms |
| Peak served, 4 CPUs | **8,710 req/s** at 9,000 offered (1.4% shed, admitted p99 113 ms), with 1.4 of the gateway's 4 cores still idle: the laptop ran out first |
| Scaling | 2,916 req/s on 1 CPU, 5,400 on 2, 8,710 on 4 |
| Billing under failure | **0 lost, 0 double-billed** in every run, including SIGKILL of the consumer mid-run (150,000 requests) and a broker restart (150,001 requests) |
| Backlog drain | 174,532 queued events billed at **28,935 events/s**, metering on 1 CPU |
| Concurrent streams (30 s each) | **6,000** open at once on a 2 GB gateway, all completed and billed once; 3,000 on 1 GB. One step further the Java heap ran out (about 150 KB per open stream, from a heap histogram) |
| On Kubernetes (k3s, k3d) | 30,001 requests at 500 req/s from a k6 Job inside the cluster: 0 errors, p99 **94.3 ms** warm, 0 lost, 0 double-billed. The cold run before it (pods 20 s old) had p99 300.9 ms while the CPU autoscaler took the gateway from 2 to 4 replicas. Both on battery, so latency is indicative |
| Redis outage | fails closed with a 503 in **0.51 s** (with Lettuce's defaults every request hung for 60.1 s) |
| Overload, before and after load shedding | p99 13.9 s with 140,264 requests never started, then admitted p99 129 ms while serving 5,011 req/s at 6,000 offered |

What each component spends per request, what broke under load in the first version, and the
before and after numbers for each fix are in [`docs/DESIGN.md`](docs/DESIGN.md).

### At larger scale (projected, not measured)

Sized from the measured CPU cost per request (gateway 0.30 to 0.42 ms, Redis 0.031 to 0.050 ms),
the measured drain rate, and about 200 bytes of Postgres per event. They assume cost stays
linear; from 1 to 2 CPUs throughput rose 1.85x, and from 2 to 4 it rose 1.6x before the laptop
became the limit.

| Traffic | Gateway cores (70% busy) | Redis | Metering instances | `usage_event` growth |
|---|---|---|---|---|
| 5,400 req/s (466 million/day) | 2, measured | one, 0.22 cores | one | 93 GB/day |
| 10,000 req/s | 5 to 6 | one, 0.3 to 0.5 cores | one (35% of its drain rate) | 173 GB/day |
| 50,000 req/s | 21 to 30 | 2 to 3 shards: one main thread tops out near 20,000 to 32,000 admissions/s | 2 | 864 GB/day |

At 50,000 req/s, Redis is the first hard limit (shard by tenant; the keys are already Cluster
safe) and Postgres needs monthly partitions with raw rows aged out once rollups are final.


## How it works

```
 client ──► gateway ──► upstream LLM (mock-upstream in tests and benchmarks)
             │  ▲
   Lua script│  │quota (read)
             ▼  │
            Redis ◄──────────── quota write-back ───┐
             │                                      │
             └─ UsageEvent ─► Kafka usage-events ─► metering ─► Postgres
                 (key = tenant)  6 partitions      batch tx     usage_event (PK event_id)
```

1. **Auth.** API keys are stored as SHA-256 digests and cached for 60 s.
2. **Admission.** One Lua script does the token-bucket rate limit and the monthly quota check
   atomically, on Redis's clock, so every replica agrees. If Redis is unreachable the gateway
   fails closed with a 503 inside 500 ms.
3. **Load shedding.** An in-flight limit (128 permits per CPU) answers the excess with an
   immediate 503 and `Retry-After`, so admitted requests stay fast under overload.
4. **Relay.** Plain blocking code on virtual threads. Streams are relayed event by event; a
   client that disconnects mid-stream is still billed for the full completion (status 499).
5. **Usage event.** Keyed by tenant, handed to a dispatcher thread so a slow broker never
   blocks a request.
6. **Metering.** At-least-once delivery plus an idempotent sink: offsets are committed only
   after the Postgres transaction, and `usage_event`'s primary key turns redelivery into a
   no-op. Only new rows feed the per-minute rollups and monthly totals, which are written
   back to Redis with a set-if-greater script.

[`docs/DESIGN.md`](docs/DESIGN.md) covers delivery semantics, partitioning, every failure mode
and what measuring it found, in the order the fixes went in.

## Layout

| Path | What |
|---|---|
| `gateway/` | The proxy: auth, admission (one Redis Lua script), load shedding, SSE relay, usage events |
| `metering/` | Kafka batch consumer: idempotent insert, per-minute rollups, monthly totals, quota write-back, DLT |
| `mock-upstream/` | OpenAI-shaped upstream with log-normal latency, so benchmarks cost nothing and repeat |
| `usage-common/` | The `UsageEvent` contract, topic names and the Flyway schema |
| `loadtest/` | k6 scripts, a virtual-thread stream client, and `bench.py`, which runs every benchmark and chaos test and reconciles the results |
| `deploy/docker-compose.yml` | The full stack, with Jaeger, Prometheus and a provisioned Grafana dashboard |
| `deploy/k8s/` | Manifests (probes, HPA, PDB, Traefik ingress), a k3d script and an in-cluster bench |
| `deploy/terraform/` | AWS: a one-time `bootstrap` (OIDC role, ECR, reports bucket, budget alarm) and a per-run `stack` |
| `docs/DESIGN.md` | Delivery semantics, partitioning, failure modes, what broke under load and why |
| `results/` | Every measured number (`summary.json`), the raw k6 summaries, CPU profiles and `index.html` |

## Run it

Needs Docker, JDK 21 and Maven.

```bash
make test            # 22 tests: 17 integration tests against real Postgres, Kafka and Redis (Testcontainers), 5 unit tests
make up              # build jars and images, start the stack
curl -s localhost:8080/v1/chat/completions -H 'Authorization: Bearer sk-dev-demo' \
  -H 'Content-Type: application/json' -d '{"model":"mock-small","messages":[{"role":"user","content":"hi"}]}'
```

Grafana is on http://localhost:3001 (anonymous viewer), Jaeger on http://localhost:16686 and
Prometheus on http://localhost:9090. The dev keys (`sk-dev-demo`, `sk-dev-hot`, `sk-dev-capped`,
`sk-dev-lt-01` to `sk-dev-lt-50`) are seeded by migration for the local stack only.

```bash
TRACE_SAMPLE_RATIO=0.01 python3 loadtest/bench.py steps   # saturation search
python3 loadtest/bench.py overload                        # above capacity, then recovery
python3 loadtest/bench.py consumer-kill                   # SIGKILL metering mid-run, reconcile
python3 loadtest/bench.py broker-restart                  # restart Kafka mid-run, reconcile
GATEWAY_CPUS=4 python3 loadtest/bench.py capacity         # CPU and memory per component per request
python3 loadtest/bench.py drain                           # how fast a Kafka backlog becomes billing rows
python3 results/build_page.py                             # render results/index.html
make k3d-up && deploy/k8s/bench-on-cluster.sh             # the same stack on k3s
```

The concurrency test needs the slow mock and a raised in-flight limit:

```bash
MOCK_MEDIAN_MS=30000 MOCK_SIGMA=0 MOCK_MAX_MS=60000 MOCK_STREAM_CHUNKS=30 \
  docker compose -f deploy/docker-compose.yml up -d --force-recreate mock-upstream
MAX_IN_FLIGHT=30000 docker compose -f deploy/docker-compose.yml up -d --force-recreate gateway
MOCK_MEDIAN_MS=30000 MOCK_STREAM_CHUNKS=30 MAX_IN_FLIGHT=30000 COUNTS=2000,5000,10000,20000 \
  python3 loadtest/bench.py streams
```

`bench.py` refuses to run on battery power: macOS throttles the CPU and the numbers stop being
comparable (one run at 17% battery spent 2.4x the CPU per request and was discarded).

## AWS

`deploy/terraform` provisions one Graviton EC2 instance running k3s in its own VPC (no NAT
gateway, no SSH, SSM only), and the `aws-bench` workflow builds arm64 images, applies the stack,
runs the in-cluster bench, uploads the report to S3 and destroys the stack in the same job. It
assumes an IAM role through GitHub OIDC, so there are no long-lived keys. The instance also
schedules its own termination after 90 minutes in case a destroy never runs, and a budget alarm
covers the account.

**Status: validated, not yet applied.** Both modules pass `terraform fmt -check` and
`terraform validate`. Nothing has been deployed to AWS yet, so there is no AWS run time or cost
to report. Apply `bootstrap` once in a personal account, then run the workflow.

## Limits, stated plainly

- The upstream is a mock with realistic latency, not a paid LLM API.
- Every setup runs one Kafka broker and one Redis. Production needs replicas
  (`replication.factor=3`, `min.insync.replicas=2`; Redis with a replica and failover).
- A gateway crash loses events still in memory (the dispatch queue and producer buffer:
  normally milliseconds of traffic). An outbox would close that gap.
- Quota can overshoot by roughly consumer lag times request size, because admission reads the
  last committed monthly total.
