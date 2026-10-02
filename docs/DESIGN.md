# Design

## What it does

A tenant calls the gateway exactly as it would call an OpenAI-compatible API. The gateway
authenticates the tenant, admits or rejects the request (rate limit, monthly token quota),
forwards it upstream with the platform's own credential, relays the reply (buffered or SSE
streamed), and emits one usage event per forwarded request. A separate metering service turns
those events into billing rows and quota state.

```
 client ──► gateway ──► upstream LLM (mock-upstream in benchmarks; also run against Ollama)
             │  ▲            max_tokens = what the quota can still pay for
 reserve,    │  │
 settle,     ▼  │
 release    Redis: rate limit, quota, leases ◄── quota write-back, lease reaper ──┐
             │                                                                  │
             └─ UsageEvent ─► Kafka usage-events ─► metering ─► Postgres ────────┘
                 (key = tenant)  6 partitions      batch tx     usage_event (PK event_id)
                                    │                            usage_rollup_minute
                                    └► usage-events.DLT          tenant_month_usage
```

## Request path (gateway)

1. **Auth.** `Authorization: Bearer <key>` is hashed with SHA-256 and looked up in `api_key`.
   Postgres holds digests only. Hits are cached for 60 s and misses for 10 s (Caffeine), so a
   client hammering a bad key costs at most one query per key per 10 s.
2. **Admission, one Redis round trip.** The gateway counts the prompt's tokens locally, then
   `admission.lua` checks the monthly quota, refills the token bucket and reserves the request's
   worst-case cost in a lease, atomically (see "Leases" below). It uses Redis's `TIME`, not the
   gateway's clock, so every replica refills the same bucket identically. Every key carries the
   `{tenant}` hash tag, so the script stays valid on Redis Cluster.
3. **Forward.** `java.net.http.HttpClient` on virtual threads (Spring MVC with
   `spring.threads.virtual.enabled`). Blocking I/O per request is cheap, so the code stays
   straight-line instead of reactive.
4. **Streaming.** For `stream: true` the gateway sets `stream_options.include_usage` upstream,
   reads token counts from the final chunk, and strips that chunk again if the client did not
   ask for it. If the client disconnects mid-stream, the gateway keeps draining the upstream
   stream: the provider bills the full completion, so the tenant must be billed for it too.
   The event is recorded with status 499. Two things made this harder than it looks, and a
   test pins each one (`GatewayIntegrationTest`):
   - Spring hands a `StreamingResponseBody` a `StreamUtils.NonFlushingOutputStream`, so
     `out.flush()` does nothing and every event sat in the servlet buffer until the stream
     ended. The relay flushes with `HttpServletResponse.flushBuffer()` after each event.
     The test asserts the events of an upstream that spaces them 250 ms apart reach the client
     spread over more than 1.5 s, not all at once.
   - When the client disconnects, Spring cancels the streaming task with an interrupt. With the
     upstream read on that thread, the interrupt aborted the read before the final usage chunk,
     so the event was recorded as a 502 with zero tokens: an under-billed completion. The
     upstream is now read on its own virtual thread (`SseRelay`), which hands lines to the
     client writer through a 256-line bounded queue. The interrupt stops only the writer. The
     drain finishes and bills the full usage with status 499, and a slow client still applies
     backpressure upstream through the queue.
5. **Usage event.** `UsageEvent{eventId, tenantId, model, tokensIn, tokensOut, latencyMs,
   status, streamed, occurredAt, usageSource, estTokensIn, estTokensOut}`, keyed by tenant. The
   event id is the request's lease id, and the gateway sends it upstream as `X-Request-Id`, so a
   provider's own logs reconcile against billing row by row. `usageSource` says where the counts
   came from: `provider` (its usage report), `estimated` (the gateway's tokenizer, when the provider
   reported nothing), `none` (an error response) or `lease_expired` (see "Leases"). The request thread only offers it to a
   bounded in-memory queue (50,000 events); one dispatcher thread serializes and sends. Rejected
   requests (401, 429) are never forwarded and never billed. Upstream failures are recorded
   with zero tokens so error rates appear in the rollups.

## Delivery semantics

**At-least-once delivery plus an idempotent sink, not Kafka exactly-once.**

- The producer is idempotent (`enable.idempotence=true`, `acks=all`), so broker-side retries
  do not create duplicates within a producer session.
- The consumer commits offsets only after the Postgres transaction commits. A crash between
  commit and offset commit redelivers the batch.
- The redelivered rows hit `usage_event`'s primary key and are skipped by
  `ON CONFLICT DO NOTHING ... RETURNING event_id`. Only the rows that were actually inserted
  feed `usage_rollup_minute` and `tenant_month_usage`, in the same transaction. Replaying any
  batch any number of times changes nothing.

Why not Kafka transactions (EOS)? EOS covers Kafka-to-Kafka. The sink here is Postgres, so EOS
would still need an idempotent write or a two-phase commit. The primary key gives the same
guarantee with one round trip and no coordinator.

**Poison messages.** A record that does not parse, or fails `UsageEvent.validationError()`
(negative tokens, missing tenant, impossible status), goes to `usage-events.DLT` with a
`dlt-reason` header. The DLT send is awaited before the batch commits, so a poison record is
never silently dropped. Any other failure (database down, DLT unavailable) fails the whole
batch, and the container retries it with backoff from 0.5 s to 10 s, indefinitely. Skipping
the batch would lose billable events, and replay is safe.

**Quota state.** Postgres is the source of truth (`tenant_month_usage`). After each commit,
metering writes the new monthly total to Redis with a "set if greater" script, so a late or
replayed write can never roll a tenant's usage backwards. A failed Redis write does not fail
the batch: a resync job copies the month's totals every 30 s.

## Leases: a hard quota, and billing that survives a gateway crash

The first version admitted a request by comparing the quota with the last total metering had
committed. Requests in flight, and events still in Kafka, were invisible to it, so a tenant hit
hard could spend well past its quota before the totals caught up. `bench.py quota-race` measures
exactly that: one tenant with a 300,000-token monthly quota, hit for 10 s at each rate, every
figure read from Postgres after the consumer lag reaches zero. The first version billed 303,192
tokens at 500 req/s (1.06% past the quota), 314,401 at 2,000 (4.8%) and 342,982 at 5,000
(14.33%). It also lost whatever usage was still in a gateway's memory when the gateway died.

Each request now holds a **lease** in Redis from admission until Kafka has its usage event.

1. **Reserve.** `admission.lua` counts what is used plus what admitted requests might still use,
   so concurrent requests cannot jointly spend past the quota. It reserves the prompt (counted
   locally, plus a margin, see "Counting tokens") and the output the request may generate. The
   output is granted only as far as the quota can still pay for it, and the gateway sends the
   grant upstream as `max_tokens` (or `max_completion_tokens` for reasoning models), so the
   provider itself cannot generate past the quota. A lowered limit is reported in
   `X-Granted-Max-Tokens`. The lease records the reservation and a template event.
2. **Settle.** When the response is complete, `settle.lua` releases the reservation and adds the
   real usage to the month's total in one step, so the quota never sees a request twice or not at
   all. The lease now holds the exact usage event.
3. **Release.** When Kafka acknowledges the event, `LeaseReleaser` deletes the lease. The
   acknowledgement arrives on the producer's I/O thread, which must never wait on Redis, so the
   deletes are queued and pipelined in batches of 1,000 on their own thread.
4. **Reap.** Metering's `LeaseReaper` (every 10 s; 5 s in compose) claims leases past their ttl
   with `reap.lua`, atomically, into a per-tenant list. A settled lease is published as it is. An
   unsettled one belongs to a request whose gateway died mid-flight, so it is billed at its
   reservation, the most the request could have cost (`status 0`, `usage_source =
   lease_expired`), and its reservation becomes usage in the same script. A reaped record is
   removed only after Kafka acknowledges it.

Every step is safe to repeat. The reaper publishes under the lease id, which is the event id the
gateway used, so when both get through the sink's primary key keeps one row. Claiming is atomic,
and a reaped record is removed by value (`LREM`), so two metering replicas cannot remove each
other's work. The lease ttl must exceed the longest request by 30 s, which is checked at startup:
otherwise a slow request's lease would be reaped and billed at its reservation, and the real event
dropped as a duplicate of it.

**Measured, the same quota race:** 299,981 tokens billed at 500 req/s, 299,988 at 2,000 and
299,988 at 5,000. That is 0 tokens past the quota, and more than 99.99% of it used. Both versions
ran on battery power on the same machine the same morning.

| Failure | What happens now |
|---|---|
| Gateway killed mid-request | The lease expires and the request is billed at its reservation, an upper bound, flagged `lease_expired`. Measured below. |
| Gateway dies after settling, before Kafka acknowledges | The reaper publishes the exact event the lease holds. |
| Gateway dies after Kafka acknowledged, before the release | The reaper republishes; the sink drops the duplicate. |
| Kafka unreachable past the producer's 120 s | The event stays in its lease and the reaper republishes it later. A tenant with 100,000 open leases is refused with 503 `billing_backlog` rather than served unbillably. |
| Settling fails (Redis blip) | The event is still published, but the lease is kept, and the reaper turns the reservation into usage. Redis then counts the reservation, not the real usage, for that request: conservative, and rare. |
| Redis restarts | Redis keeps an append-only log fsynced every second, so leases survive. Up to a second of lease writes can be lost; those requests are still billed by the gateway that served them, unless it dies in the same second. |

**Measured, `bench.py gateway-kill`:** 1,000 req/s for 60 s, with the gateway SIGKILLed at
t=20 s and started again 5 s later (healthy at 31.5 s). The mock provider records the request id
of every request it served, and the gateway sends its lease id as that request id, so every
served request can be looked up in Postgres afterwards. The provider served 47,661 requests and
Postgres holds 47,661 billing rows, one for each: none served and unbilled, none billed and never
served. 47,620 rows carry the provider's own usage and match it exactly. The other 41 were in
flight when the gateway died; their leases expired and were billed at their reservations, 5,270
tokens more than those requests actually used (0.07% of the 7.19 million tokens served).
Everything was billed by t=146 s, about two minutes after the kill, which is the lease ttl in
compose. Run on battery power.

The same run against the first version, with the provider counting requests instead of ids
(that version sends none): the provider served 49,131 requests and 49,085 were billed. 46
requests and 6,477 tokens were never billed. Four of them had already reached the client as
complete 200 responses; their events were still in the gateway's memory when it died. The other
42 were in flight. Back to back with the run above, on battery.

The costs, stated plainly. A reservation is the worst case, so near the end of a quota a burst of
concurrent requests is refused while earlier requests still hold their reservations, even if they
end up using less. Those refusals are 429 `quota_reserved` with `Retry-After: 1`, distinct from
429 `insufficient_quota`, because a retry usually fits once the earlier requests settle. And every
request now does more work: the admission script writes a lease, a settle script and a pipelined
release follow it, and the gateway counts the prompt and serializes the event twice.

**What that costs, measured.** The 2-CPU gateway before leases (v1) and with them (v2), back to
back on mains power in the order v2, v1, v2, each after a 30 s warm-up at 1,000 req/s
(`capacity-2cpu-v1-ac`, `-v2-ac-a`, `-v2-ac-b` in `results/summary.json`):

| | v1 | v2 (two runs) |
|---|---|---|
| Served at 6,000 req/s offered | 3,879 req/s | 3,394 and 3,122 req/s, 12 to 20% less |
| Gateway CPU per request at 4,000 offered | 0.48 ms | 0.55 and 0.58 ms |
| Redis CPU per request at 4,000 offered | 0.056 ms | 0.084 ms, 50% more |
| Gateway memory at saturation (1 GiB container) | 863 MiB | 1,016 and 1,023 MiB |

The machine was busier than for the 5,400 req/s run in the results table (load average 6 to 8, from
other work on the laptop): v1 itself peaked at 3,879 req/s here. So the comparison is the result,
not the absolute numbers. The memory is mostly heap the JVM commits under allocation pressure, up
to its cap of 65% of the container; the live data is small, and the tokenizer's two vocabularies
are about 50 MB of it (class histogram). It is still too close to the limit: give a lease gateway
at least 1.25 GiB.

Right after a restart the lease version sheds more while the JIT compiles its longer hot path. In
the first 30 s at 1,000 req/s, run in the order v1, v2, v2, v1, v1 shed 1,349 and 1,411 requests
and v2 shed 2,130 and 1,958; in the next 30 s both shed none, with admitted p99 at 92 ms. A JFR
recording of a cold v2 showed the container throttled in 92% of its scheduler periods, the JIT
compiler threads among the busiest. Slow-start at the load balancer covers this.

## Counting tokens

The gateway counts tokens itself with jtokkit, using OpenAI's vocabularies (`o200k_base`, or
`cl100k_base` for gpt-4 and gpt-3.5), plus OpenAI's chat framing: 3 tokens per message and 3 to
prime the reply. That count sizes the prompt reservation, and bills a response that arrives
without a usage report: a stream cut at the gateway's maximum duration, an upstream that drops
the connection, or a provider that never reports usage. Those rows are marked `estimated`. The
provider's own report is used whenever there is one, and the gateway's count is kept next to it
(`est_tokens_in` always, `est_tokens_out` on a 1% sample by default) so drift stays visible.

For OpenAI models the count is the provider's own vocabulary. For anything else it is an
approximation, and how far off is measured rather than assumed. `bench.py real-model` sends 200
requests of ten prompt shapes (system messages, multi-turn, code, JSON; half streamed) to
qwen2.5:0.5b on Ollama through the gateway, with every output counted:

| Gateway's count vs the model's report | median | 10th percentile | 90th percentile | widest |
|---|---|---|---|---|
| Prompt tokens | 47.8% low | 53.7% low | 4.9% low | 73.3% low |
| Output tokens | 1.6% low | 9.0% low | exact | 18.2% either way |

All 200 responses carried the model's own usage report, so every row was billed from it; the
gateway's count only decided the reservation.

The output count was close (median 1.6% low). The prompt count was far off (median 47.8% low),
and not because of the vocabulary: Ollama's template for this model adds a default system prompt
when the request has none, so a one-word message the gateway counts as 8 tokens is billed by the
model as 30. The first calibration learned a ratio (provider count over local count), which went
to 3.09 and then reserved every prompt at three times its size. Nothing went past the quota, but in
a race of 60 concurrent requests against a 3,000-token quota only 1,545 tokens were used. An extra
fixed number of tokens is the right model for a chat template, so `PromptCalibration` now learns
the excess in tokens per model: it jumps to any larger excess it sees, since reserving too little
is what could spend past a quota, and decays by 1% per request when later requests need less.

**Measured with the excess model**, same model and same race: it learned an excess of 20.4
tokens, and the 60 requests used 2,982 of the 3,000 tokens, 0 past the quota. 22 were served (10
of them with a lowered `max_tokens`) and 38 refused once the quota was spent; the 18 tokens left
are fewer than any prompt here needs. Requests refused with `quota_reserved` retried after
waiting, 709 times in total across the race, which is the burst cost described above.

The quota guarantee does not rest on the estimate. Output cannot pass the grant, because the
provider is told the limit. The prompt can be under-reserved only by the provider counting more
than the learned excess on a request near the end of a quota, and only for the first requests to
a model before it has been seen.

## Partitioning and ordering

Events are keyed by `tenantId`, so one tenant's events land in one partition and are applied
by one consumer thread in order. That is why the rollup and monthly upserts never contend
across threads. Within a batch, rows are sorted by key before the multi-row upserts, so two
transactions that do overlap (briefly, during a rebalance) take locks in the same order and
cannot deadlock.

**The hot-tenant problem.** Keying by tenant means one very large tenant is bounded by one
partition's throughput. The usual fixes, in order of cost:

1. Pre-aggregate in the gateway (per tenant, per second) and publish rollup deltas instead of
   raw events. This cuts volume by orders of magnitude but loses per-request audit rows, so it
   only fits if the audit trail lives elsewhere.
2. Key by `tenantId + bucket` (for example `hash(eventId) % 8`) for tenants on an allow-list.
   This gives up per-tenant ordering, which is safe here because every write is commutative
   (sums) or idempotent (inserts by id).

## Backpressure and failure modes

| Failure | What happens | Bound |
|---|---|---|
| Upstream slow | Requests wait on virtual threads; the per-request timeout is 120 s | Tomcat `max-connections` 20,000 |
| Upstream down | 502 or 504 to the client, event recorded with status and zero tokens | Connect timeout 2 s |
| Kafka broker down | Events wait in the dispatch queue and the producer buffer; the producer retries | Queue of 50,000 events, then `buffer.memory` 32 MB and `delivery.timeout.ms` 120 s |
| Dispatch queue full | The event is counted as failed and logged to `usage.unpublished`; its lease still holds it, and the reaper publishes it after the lease ttl | About 25 s of traffic at 2000 requests/s |
| Kafka down longer than 120 s | The send fails and is counted in `gateway_usage_events_total{result="failed"}`; the lease keeps the event and the reaper republishes it once Kafka is back | 100,000 open leases per tenant, then 503 `billing_backlog` |
| Gateway crash | Requests in flight are billed at their reservation when their leases expire; settled but unacknowledged events are republished from their leases | Lease ttl (11 min by default, 2 min in compose) plus one reap interval |
| Metering down | Events accumulate in Kafka; quota checks see stale totals | Topic retention |
| Postgres down | Metering retries the batch indefinitely; the gateway keeps serving cached tenants | Lag grows until it recovers |
| Redis down | Admission fails within the 500 ms command timeout and the gateway returns 503 `admission_unavailable` with `Retry-After: 1` (fail closed) | 500 ms per request; Redis HA is out of scope |

Fail-closed on Redis is a deliberate choice. Failing open would let a tenant exceed its quota
without bound during an outage, and quota is a billing control. Failing closed has to be fast,
though. With Lettuce's defaults (a 60 s command timeout, and commands queued for the reconnect
while the connection is down), stopping Redis made every request hang: `curl` gave up at its
10 s cap, and each hung request held an in-flight permit, so 256 of them would have blocked all
traffic, including tenants the gateway could have rejected cheaply. With a 500 ms timeout the
same outage answers 503 in 0.51 s, and the first request after Redis returns gets a 200.
`GatewayIntegrationTest` pauses the Redis container and fails if the answer takes 3 s or more
(with the default timeout it took 60.1 s).

## Overload: what measuring it found

The first version did not degrade gracefully. These are the fixes in the order they went in,
each measured with the same k6 steps on the compose stack (gateway limited to 2 CPU / 1 GB; all
records are in `results/summary.json` and `results/before-limiter.json`).

1. **No bound on admitted work.** Above capacity, requests queued without limit, each holding
   its body, its parsed JSON and an upstream connection. A 6000 requests/s step served 125,684
   responses at p99 13.9 s, and k6 could not even start 140,264 of its iterations. Fix:
   `InFlightLimitFilter`, a semaphore in front of `/v1/`. The excess gets an immediate 503 with
   `Retry-After: 1`. A stream holds its permit until its upstream drain finishes, even after
   the client has gone, because that is when its upstream connection is released (the permit is
   reference-counted; `InFlightLimitIntegrationTest` covers both cases and fails if the drain's
   hold is removed).
2. **SerialGC, chosen silently.** With fewer than 2 CPUs or less than 1792 MB the JVM is not
   "server class" and picks SerialGC. Pauses reached 447 ms (old generation) and 342 ms
   (young). Each pause let in-flight work jump past the limit, so the gateway shed in bursts: at
   2000 requests/s, 515 non-2xx and p99 500 ms. Fix: the GC is set explicitly in the Dockerfile.
3. **G1 alone made it worse.** One full compaction pause of 1.03 s, the container at 1014 MiB
   of 1 GiB, and at 2000 requests/s 5,935 non-2xx with p99 2.87 s. A heap histogram after a full
   GC showed about 150 MB live, so this was not a leak.
4. **The profile.** A JFR recording at a steady 1000 requests/s put 20% of CPU samples in
   `jdk.internal.net.http.ConnectionPool$ExpiryList.remove` and the `LinkedList` iterator it
   walks (`results/profiles/1000rps-before-pool-cap.txt`). The JDK `HttpClient` pool has no size
   cap by default and keeps idle connections for 20 minutes, and its expiry list is scanned on
   checkout. A stall opens hundreds of upstream connections, they stay pooled, and every later
   request pays for the longer scan: more CPU, then more latency, more concurrency and more
   connections. The same loop is the likely reason the 6000 requests/s collapse in (1) persisted
   after the load stopped (likely, not shown: that run was not profiled). Fix:
   `jdk.httpclient.connectionPoolSize=256` and a 30 s keep-alive. In the next full steps run there
   was no full GC, the largest young pause was 229 ms, and the container sat at 728 MiB instead
   of 1014 MiB. (GC figures here are Micrometer's rolling two-minute maximum read at the end of
   each run, so they are lower bounds on the run's worst pause.)
5. **Sizing the limit.** By Little's law, about 2000 requests/s at about 45 ms each needs about
   90 requests in flight. A limit of 256 leaves roughly 3x headroom and caps queueing delay near
   256 / 2000 = 128 ms. Measured at 2000 requests/s: a limit of 256 gave 874 non-2xx (1%) and p99
   160 ms (113 ms for the requests it admitted); a limit of 512 gave 5,454 non-2xx and p99 773 ms.
   The larger limit lets the queue grow before shedding, so more requests wait and each waits
   longer. 256 was the default for 2 CPUs; item 7 is why it became 128 per CPU.
6. **Kafka on the request path.** `KafkaProducer.send()` is not fully asynchronous: after a
   leader change it blocks its caller while it refreshes metadata, for up to `max.block.ms`.
   With the send on the request thread, restarting the broker at 1000 requests/s gave 2,957
   non-2xx responses, 1,342 dropped iterations and p99 2.73 s (`broker-restart-sync-publish` in
   `results/summary.json`). Fix: the request thread only offers the event to a bounded queue,
   and one dispatcher thread sends. That exposed the next gap. On a heavily loaded host (load
   average 16) the metadata wait outlasted `max.block.ms` (5 s), and the dispatcher dropped one
   event: 136,775 successful responses, 136,774 billed (`results/raw/before-retry/`). It was
   logged to `usage.unpublished`, so it was recoverable, but a billing pipeline should not need
   that. Fix: the dispatcher retries a send that timed out on metadata, with backoff from 100 ms
   to 2 s, until shutdown. `UsagePublisherTest` fails without the retry. In the final runs the
   broker came back inside the 5 s window, so the retry did not fire there; the unit test is
   what shows it works.
7. **The limit has to scale with CPUs.** A fixed limit caps throughput at limit / mean latency
   whatever the hardware. With 4 CPUs and the limit at 256, offering 8000 requests/s served
   5,762/s (about 256 / 44 ms) and shed 100,698 requests while the gateway used 2.4 of its 4
   cores. With the limit at 512 the same 4 CPUs served 7,785/s at 8000 offered and 8,710/s at
   9000 offered (1.4% shed, admitted p99 113 ms), still with 1.4 cores to spare. Item 5 is the
   other half: 512 on 2 CPUs only lengthened the queue. So the default is now 128 permits per
   available CPU (128, 256 and 512 at 1, 2 and 4 CPUs, read from the `gateway_inflight_limit`
   gauge), and `MAX_IN_FLIGHT` still overrides it. One CPU saturates near 2,900 requests/s,
   which 128 permits at 44 ms already allow.

The final chaos runs, each after a 30 s warm-up, at 1000 requests/s for 150 s:

| Fault | Requests | Non-2xx | p99 | Lost | Double-billed | Lag back under 100 |
|---|---|---|---|---|---|---|
| `docker kill -s KILL` metering for 45 s | 150,000 | 0 | 96.5 ms | 0 | 0 | 16.2 s after restart (peak 56,143) |
| `docker restart` the Kafka broker | 150,001 | 0 | 92.5 ms | 0 | 0 | 3.9 s (peak 3,325) |

**Cold start.** A freshly started gateway cannot take 1000 requests/s for its first seconds
while the JIT compiles: the 30 s warm-up step shed 2,007 of 30,001 requests with the limit at
256. A real rollout needs slow start at the load balancer or warm-up traffic before the pod
takes its full share; readiness alone only says the process is up. The chaos runs first had no
warm-up of their own, so a run that started on a freshly recreated gateway counted 1,258 cold
start 503s, all in its first 10 s and all before the fault, as the fault's cost
(`results/raw/no-warmup/`). They now warm up for 30 s first.

**Measurement noise.** These are one laptop's numbers. k6, the mock upstream and the other
services share the same 10-CPU Docker VM, the host load average was 4 to 8 during the runs, and
repeated runs at 2000 requests/s varied widely while the fixes above were going in. Treat the
order of magnitude and the direction of each change as the result, not the exact figures. One
run was invalid outright: on battery at 17% macOS throttled the CPU and the gateway spent 2.4x
the CPU per request. That run was discarded and `bench.py` now refuses to start on battery.

## Long streams: memory, not CPU

A real completion streams for seconds, so what a gateway runs out of first is open connections
and heap, not requests per second. `bench.py streams` holds N streams open at once (each streamed
by the mock over 30 s in 30 chunks, started evenly over 20 s, read by a virtual-thread client)
with the in-flight limit raised out of the way:

| Gateway container (heap 65%) | Held, all completed and billed once | Next step |
|---|---|---|
| 1 GB (665 MB heap) | 3,000 (2 requests got a 503, nothing lost) | 4,000: `OutOfMemoryError: Java heap space`, the JVM exited |
| 2 GB (1.3 GB heap) | 6,000 | 8,000: the same |

CPU stayed under one core throughout. A class histogram at 3,000 streams
(`results/profiles/heap-3000-streams.txt`, live objects after a full GC) holds 495 MB against
48 MB idle, so about 150 KB per open stream. Most of it is buffers: about 77 KB of byte arrays
(Tomcat's per-connection socket, header and output buffers, the JDK `HttpClient` connection, the
relay's decoder) and about 44 KB of char arrays (Tomcat allocates a 16 KB char buffer for the
request reader and another for the response writer on every connection, though the relay uses
neither, plus the relay's 16 KB `BufferedReader`). A non-blocking relay that shares buffers would
cut this; thread-per-stream on virtual threads costs well under 10% of it (stack chunks were
9.9 MB for 3,000 streams).

Two consequences:

- **The CPU-scaled in-flight limit does not fit long streams.** 128 permits per CPU suits
  requests of about 45 ms. At 30 s each, 256 permits would serve about 8 streams/s on a 2-CPU
  gateway while memory for 3,000 sat unused. A deployment that streams long completions should
  size `MAX_IN_FLIGHT` by memory instead: roughly 0.8 x (heap - 50 MB idle) / 150 KB, which is
  about 3,300 for the 1 GB container's 665 MB heap (it held 3,000 and failed at 4,000). Splitting streamed and buffered requests into separate limits is the better fix, and is
  not done here.
- **Without that limit the failure is a cliff.** Past the heap, the JVM exits on
  `OutOfMemoryError` and every open stream fails at once. With the limit at the memory bound,
  the excess gets a fast 503 instead.

The harness had a bug of its own first: the mock had no async timeout configured, so Tomcat's
30 s default cut streams that took slightly longer than their 30 s, just before `[DONE]` (9 of
2,000, then 217 of 6,000 as load stretched the streams). The gateway billed each one, because
the upstream stream had ended. The mock now allows 300 s. The affected runs are kept in
`results/raw/mock-timeout-30s/`.

## On Kubernetes

`deploy/k8s` runs the same images on k3s: two gateway replicas behind a Service with a CPU
autoscaler (2 to 4), a PodDisruptionBudget, readiness and liveness probes, and a Traefik
ingress. `bench-on-cluster.sh` runs k6 as a Job inside the cluster and reconciles against
Postgres, so the result does not depend on the laptop's port forwarding.

| Run (500 req/s, 60 s) | Requests | Non-2xx | p50 | p99 | Gateway replicas | Lost | Double-billed |
|---|---|---|---|---|---|---|---|
| Cold start (pods 20 s old) | 30,000 | 0 | 42.2 ms | 300.9 ms | 2, then 4 | 0 | 0 |
| Warm (same pods, next run) | 30,001 | 0 | 41.3 ms | 94.3 ms | 4 | 0 | 0 |

The cold run is the cold-start problem from the overload section again: JIT compilation pushed
CPU over the autoscaler's 70% target and it added two replicas 39 s in, but the added pods were
cold as well, so p99 carried the warm-up. The warm run's p99 is 4 ms above the 90.2 ms p99 the mock alone gave under Compose.
Both runs were on battery power, so the latencies are indicative rather than comparable with
the Compose numbers.

Two things broke on the way, neither visible under Compose:

1. **Service links overrode the app's config.** Kubernetes injects `<SERVICE>_PORT` variables
   for every Service in the namespace, so the Service named `redis` set
   `REDIS_PORT=tcp://10.43.x.x:6379` and the gateway and metering both failed to start binding
   it to an int. The apache/kafka image also reads `KAFKA_*` variables as broker config. Every
   pod now sets `enableServiceLinks: false`.
2. **The image import failed and reported success.** With Docker's containerd image store,
   `docker save` writes the whole multi-platform index, k3s's `ctr` stops on layers that were
   never pulled ("content digest not found"), and `k3d image import` still prints "Successfully
   imported". The pods then sat in `ImagePullBackOff` against Docker Hub. `k3d-up.sh` now saves
   a single platform (`docker save --platform`).

## What breaks at 10x

`bench.py capacity` samples every container while k6 holds a rate, and divides CPU by the
requests served. Those per-request costs are what the next order of magnitude is sized from.
Measured on the laptop stack (the gateway at the CPU count shown, everything else at the compose
limits):

| Component | CPU per request (measured) | What one instance did |
|---|---|---|
| Gateway | 0.30 to 0.50 ms, falling as load rises | 2,916 req/s on 1 CPU, 5,400 on 2, 8,710 on 4 (laptop-bound, 1.4 cores idle) |
| Redis (one Lua script per request) | 0.031 to 0.050 ms | 0.27 cores at 8,710 req/s |
| Kafka (one broker, 6 partitions) | 0.04 to 0.10 ms from 3,000 req/s up; 0.27 to 0.36 ms at 1,000 to 2,000, where its fixed background work dominates | 0.2 to 0.5 cores in every run |
| Metering | 0.05 to 0.14 ms steady; 0.025 ms draining a backlog | drained 28,935 events/s on 1 CPU |
| Postgres | 0.05 to 0.14 ms steady | 0.33 cores while inserting 28,935 rows/s |

Postgres holds about 200 bytes per billed event, including the primary key index (84 MB for
417,865 rows).

Projections from those numbers. They assume cost stays linear and nothing else saturates first;
none of them was run at that size:

- **Gateway.** Stateless, so it scales out (an HPA is defined in `deploy/k8s`). At 0.30 to 0.42
  ms per request one core serves roughly 2,400 to 3,300 requests/s, so 50,000 requests/s needs
  roughly 21 to 30 cores at 70% utilisation. The in-flight limit already scales per CPU.
- **Redis.** Admission runs on Redis's single main thread. At 0.031 to 0.050 ms per script that
  thread saturates somewhere around 20,000 to 32,000 admissions/s, the first hard ceiling in the
  design. Past it, shard by tenant: the keys use hash tags, so they are Redis Cluster safe, and
  a tenant's bucket and quota stay on one shard.
- **Metering.** One instance drained 28,935 events/s, five times the 5,400/s one 2-CPU gateway
  produced. Six partitions allow six consumers, so roughly 170,000 events/s before partitions
  have to grow, if Postgres keeps up. Add partitions before traffic needs them: adding them
  later remaps keys, which breaks per-tenant ordering for a moment (harmless here because the
  writes are commutative).
- **Postgres.** At 5,400 requests/s `usage_event` grows by about 93 GB a day. That table needs
  monthly partitions, and raw rows should age out to object storage once the rollups are final.
  The batch insert through `unnest()` keeps it at one statement per batch.
- **Quota precision.** Exact, by reservation (see "Leases"). The price is work per request: the
  per-request costs above were measured before leases. Back to back, leases added 14 to 20% to the
  gateway's CPU per request and 50% to Redis's (see "What that costs, measured"). Scaled by those
  factors (projected, not measured), the gateway needs roughly 24 to 36 cores for 50,000 req/s, and
  Redis's single main thread tops out nearer 13,000 to 21,000 requests/s, so sharding Redis by
  tenant comes sooner.

## Not done (on purpose, for scope)

- **Persisted calibration.** Each gateway replica learns a model's prompt excess on its own and
  forgets it on restart, so the first requests after a restart reserve without it. Keeping it in
  Redis would share it across replicas.
- **Multi-broker Kafka.** The compose and k8s setups run one broker. Production would run
  three with `replication.factor=3` and `min.insync.replicas=2`.
- **Key management API.** Tenants and keys are seeded by migration for the local stack only.
