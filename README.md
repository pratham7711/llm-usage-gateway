# llm-usage-gateway

[![ci](https://github.com/pratham7711/llm-usage-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/pratham7711/llm-usage-gateway/actions/workflows/ci.yml)

An OpenAI-compatible gateway that meters LLM usage per tenant. It authenticates each tenant,
enforces a rate limit and a monthly token quota, forwards the call upstream, relays the reply
(buffered or SSE streamed), and publishes one usage event per forwarded request to Kafka. A
metering service turns those events into billing rows in Postgres with no loss and no double
billing, even when its consumer is killed or the broker restarts mid-run.

Each request holds a lease in Redis from admission until Kafka has its usage event. That makes
the quota hard (concurrent requests cannot jointly spend past it, and the provider is told the
output limit the quota can still pay for) and lets a gateway that dies mid-request still bill
what it served. Tenants can read their own live usage and cost from `GET /v1/usage`.

Java 21 (virtual threads), Spring Boot 4, Kafka, PostgreSQL, Redis, OpenTelemetry, Prometheus,
Grafana, k6, Testcontainers, Docker Compose, Kubernetes (k3s), Terraform.

## Results

Measured on one laptop (Apple M5): a Docker VM with 10 CPUs and 7.7 GB, shared by k6, the mock
upstream and the whole stack. The mock answers in about 40 ms at the median and 90 ms at p99 (log
normal, like a fast model), so every latency below includes that. Every run reconciles k6's
successful responses against Postgres row by row. Raw data: [`results/summary.json`](results/summary.json).

The throughput, latency, streaming, drain and Kubernetes rows were measured before leases were
added. Leases cost throughput, measured back to back on the same laptop (a busier day, so smaller
absolute numbers): the lease version served 12 to 20% less at saturation (3,394 and 3,122 req/s
against 3,879 on 2 CPUs), spent 14 to 20% more gateway CPU and 50% more Redis CPU per request, and
its gateway reached its 1 GiB memory limit (863 MiB before). The quota, crash and real-model rows
are the lease version. Details: [DESIGN.md, "What that costs, measured"](docs/DESIGN.md#leases-a-hard-quota-and-billing-that-survives-a-gateway-crash).

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
| Quota under a burst | One tenant with a 300,000-token quota, hit for 10 s at 500, 2,000 and 5,000 req/s: **0 tokens past the quota** at every rate (299,981 to 299,988 billed). Before leases it went 1.06%, 4.8% and 14.33% past |
| Gateway SIGKILLed mid-run | 1,000 req/s, gateway killed at t=20 s: the provider served 47,661 requests and **47,661 were billed**, matched one by one by request id. 0 unbilled, 0 billed without being served; the 41 in flight at the kill were billed at their reservation, 5,270 tokens (0.07%) over. Before leases the same kill lost 46 served requests (6,477 tokens), 4 of them already answered with a 200 |
| A real model (qwen2.5:0.5b on Ollama) | 200 requests: the gateway's output count was within 1.6% of the model's at the median, but its prompt count read 47.8% low because of the model's chat template, so the gateway learns that overhead per model (20.4 tokens here). 60 concurrent requests on a 3,000-token quota used 2,982, 0 past |

What each component spends per request, what broke under load in the first version, and the
before and after numbers for each fix are in [`docs/DESIGN.md`](docs/DESIGN.md).

### At larger scale (projected, not measured)

Sized from the measured CPU cost per request at 3,000 req/s and above, before leases (gateway 0.30
to 0.42 ms, Redis 0.031 to 0.050 ms; lighter load costs more per request, up to 0.50 ms on the
gateway). With leases, add 14 to 20% to the gateway and 50% to Redis: Redis's ceiling then comes
nearer 13,000 to 21,000 requests/s. The table below is the pre-lease sizing; it also uses the
measured drain rate and about 200 bytes of Postgres per event. It assumes cost stays linear; from
1 to 2 CPUs throughput rose 1.85x, and from 2 to 4 it rose 1.6x before the laptop became the
limit.

| Traffic | Gateway cores (70% busy) | Redis | Metering instances | `usage_event` growth |
|---|---|---|---|---|
| 5,400 req/s (466 million/day) | 2, measured | one, 0.22 cores | one | 93 GB/day |
| 10,000 req/s | 5 to 6 | one, 0.3 to 0.5 cores | one (35% of its drain rate) | 173 GB/day |
| 50,000 req/s | 21 to 30 | 2 to 3 shards: one main thread tops out near 20,000 to 32,000 admissions/s | 2 | 864 GB/day |

At 50,000 req/s, Redis is the first hard limit (shard by tenant; the keys are already Cluster
safe) and Postgres needs monthly partitions with raw rows aged out once rollups are final.


## How it works

```
 client ──► gateway ──► upstream LLM (mock-upstream in benchmarks; also run against Ollama)
             │  ▲            max_tokens = what the quota can still pay for
 reserve,    │  │
 settle,     ▼  │
 release    Redis: rate limit, quota, leases ◄── quota write-back, lease reaper ──┐
             │                                                                  │
             └─ UsageEvent ─► Kafka usage-events ─► metering ─► Postgres ────────┘
                 (key = tenant)  6 partitions      batch tx     usage_event (PK event_id)
```

1. **Auth.** API keys are stored as SHA-256 digests and cached for 60 s.
2. **Admission.** One Lua script does the token-bucket rate limit and reserves the request's
   worst case against the monthly quota, counting what is used plus what requests in flight
   hold, atomically and on Redis's clock, so every replica agrees. The prompt is counted locally
   with OpenAI's tokenizer; the output is granted only as far as the quota can pay and sent
   upstream as `max_tokens`. If Redis is unreachable the gateway fails closed with a 503 inside
   500 ms.
3. **Load shedding.** An in-flight limit (128 permits per CPU) answers the excess with an
   immediate 503 and `Retry-After`, so admitted requests stay fast under overload.
4. **Relay.** Plain blocking code on virtual threads. Streams are relayed event by event; a
   client that disconnects mid-stream is still billed for the full completion (status 499). A
   stream that ends without a usage report is billed from the text it carried, marked
   `estimated`.
5. **Settle and publish.** One script turns the reservation into real usage; the usage event,
   keyed by tenant, goes to a dispatcher thread so a slow broker never blocks a request, and
   Kafka's acknowledgement deletes the lease.
6. **Metering.** At-least-once delivery plus an idempotent sink: offsets are committed only
   after the Postgres transaction, and `usage_event`'s primary key turns redelivery into a
   no-op. Only new rows feed the per-minute rollups and monthly totals, which are written
   back to Redis with a set-if-greater script. Each row is priced at the model's price in force
   when the request happened.
7. **Lease reaper.** Metering bills any lease whose gateway died: a settled one exactly, an
   unsettled one at its reservation, the most it could have cost, flagged `lease_expired`.

[`docs/DESIGN.md`](docs/DESIGN.md) covers delivery semantics, partitioning, every failure mode
and what measuring it found, in the order the fixes went in.

## Layout

| Path | What |
|---|---|
| `gateway/` | The proxy: auth, admission and leases (Redis Lua scripts), token counting, load shedding, SSE relay, usage events, `/v1/usage` |
| `metering/` | Kafka batch consumer: idempotent insert, per-minute rollups, monthly totals, pricing, quota write-back, lease reaper, DLT |
| `mock-upstream/` | OpenAI-shaped upstream with log-normal latency, so benchmarks cost nothing and repeat; records which request ids it served, so crash tests can reconcile them |
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
make test            # 41 tests: 26 integration tests against real Postgres, Kafka and Redis (Testcontainers), 15 unit tests
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
VERSION=v2 python3 loadtest/bench.py quota-race           # one tenant, fixed quota, 500 to 5,000 req/s
VERSION=v2 python3 loadtest/bench.py gateway-kill         # SIGKILL the gateway mid-run, reconcile every served request
MODEL=qwen2.5:0.5b python3 loadtest/bench.py real-model   # token-count drift and the quota race against a real model
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

The real-model run needs [Ollama](https://ollama.com) with the model pulled, and the gateway
pointed at it with every output counted:

```bash
UPSTREAM_BASE_URL=http://host.docker.internal:11434 ESTIMATE_SAMPLE_RATE=1.0 \
  docker compose -f deploy/docker-compose.yml up -d gateway
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

- Capacity and failure benchmarks use a mock upstream with realistic latency. Token counting and
  the quota were also run against a real model (qwen2.5:0.5b on Ollama), but not a paid API.
- Every setup runs one Kafka broker and one Redis. Production needs replicas
  (`replication.factor=3`, `min.insync.replicas=2`; Redis with a replica and failover).
- A request whose gateway is killed mid-flight is billed at its reservation, an upper bound, not
  at what it actually used. The row says so (`lease_expired`).
- Near the end of a quota, a burst is refused while earlier requests hold worst-case
  reservations, even if they use less. Those refusals are 429 `quota_reserved` with
  `Retry-After: 1`, and a retry usually fits.
- The learned prompt overhead per model lives in memory, so after a restart the first requests to
  a model with a chat template can reserve too little prompt until it has seen one response.
- Redis fsyncs its append-only log every second, so a Redis crash can lose up to a second of
  lease writes.
