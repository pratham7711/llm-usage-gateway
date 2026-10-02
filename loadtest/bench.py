#!/usr/bin/env python3
"""Benchmark and chaos runner for the local docker-compose stack.

  python3 loadtest/bench.py steps       saturation search with constant-arrival-rate steps
  python3 loadtest/bench.py baseline    the same rates straight at the mock upstream (gateway overhead)
  python3 loadtest/bench.py overload    fixed rates above capacity (no early stop), then a recovery step
  python3 loadtest/bench.py burst       10x bursts over a baseline
  python3 loadtest/bench.py consumer-kill   SIGKILL metering mid-run, measure lag recovery, reconcile
  python3 loadtest/bench.py broker-restart  restart Kafka mid-run, reconcile
  python3 loadtest/bench.py capacity    fixed rates while sampling every container's CPU and memory
  python3 loadtest/bench.py drain       stop metering, build a Kafka backlog, time how fast it is billed
  python3 loadtest/bench.py streams     thousands of slow streams open at once (needs the slow mock)
  python3 loadtest/bench.py smoke       20 s run + reconciliation, non-zero exit on any mismatch (CI)
  python3 loadtest/bench.py quota-race  one tenant with a fixed token quota, hit hard: how far past it is billed
  python3 loadtest/bench.py gateway-kill    SIGKILL the gateway mid-run, then reconcile what the provider
                                            served against what was billed, request by request
  python3 loadtest/bench.py real-model      a real local model (Ollama) behind the gateway: tokenizer drift
                                            against the model's own usage, and a quota race it must not pass

Every run appends its numbers to results/summary.json. Nothing here is estimated: each figure is
read from k6's summary, from Postgres, or from the services' Prometheus endpoints.
"""
import json
import os
import platform
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = ["docker", "compose", "-f", os.path.join(ROOT, "deploy", "docker-compose.yml")]
RAW = os.path.join(ROOT, "results", "raw")
SUMMARY = os.path.join(ROOT, "results", "summary.json")
P99_SLO_MS = float(os.environ.get("P99_SLO_MS", "250"))


def sh(cmd, timeout=900, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {r.stderr[-800:]}")
    return r.stdout


def k6(script, name, env, timeout=900):
    args = COMPOSE + ["run", "--rm"]
    for k, v in {**env, "NAME": name}.items():
        args += ["-e", f"{k}={v}"]
    args += ["k6", "run", "--quiet", f"/scripts/{script}"]
    path = os.path.join(RAW, name + ".json")
    if os.path.exists(path):
        os.remove(path)
    out = sh(args, timeout=timeout)
    line = next((l for l in out.splitlines() if l.startswith(name + ":")), out.strip()[-300:])
    print("  " + line, flush=True)
    # A summary k6 failed to write must not be replaced by an older run's file of the same name.
    if not os.path.exists(path):
        raise RuntimeError(f"k6 wrote no summary to {path} (is {RAW} writable by the k6 container?)")
    with open(path) as f:
        return parse_k6(json.load(f))


def parse_k6(data):
    m = data["metrics"]
    d = m["http_req_duration"]["values"]
    okd = m.get("http_req_duration{expected_response:true}", {}).get("values", {})
    total = m["http_reqs"]["values"]["count"]
    return {
        "ok_p50_ms": round(okd["med"], 1) if okd else None,
        "ok_p99_ms": round(okd["p(99)"], 1) if okd else None,
        "requests": total,
        "rps": round(m["http_reqs"]["values"]["rate"], 1),
        "ok": int(m.get("ok_responses", {}).get("values", {}).get("count", 0)),
        "non2xx": int(m.get("non2xx_responses", {}).get("values", {}).get("count", 0)),
        "dropped": int(m.get("dropped_iterations", {}).get("values", {}).get("count", 0)),
        "p50_ms": round(d["med"], 1),
        "p95_ms": round(d["p(95)"], 1),
        "p99_ms": round(d["p(99)"], 1),
        "max_ms": round(d["max"], 1),
    }


def psql(sql):
    return sh(["docker", "exec", "llmgw-postgres-1", "psql", "-U", "usage", "-d", "usage", "-At", "-c", sql]).strip()


def reset_usage():
    psql("truncate usage_event, usage_rollup_minute, tenant_month_usage")
    sh(["docker", "exec", "llmgw-redis-1", "sh", "-c",
        "redis-cli --scan --pattern 'quota:*' | xargs -r redis-cli del >/dev/null; "
        "redis-cli --scan --pattern 'lease:*' | xargs -r redis-cli del >/dev/null"])


def prom_counter(url, name, labels):
    body = urllib.request.urlopen(url, timeout=10).read().decode()
    total = 0.0
    for line in body.splitlines():
        if line.startswith(name + "{") and all(f'{k}="{v}"' in line for k, v in labels.items()):
            total += float(line.rsplit(" ", 1)[1])
    return total


def in_flight_limit():
    """The limit the running gateway reports (MAX_IN_FLIGHT, or 128 per CPU when unset)."""
    try:
        return int(prom_counter("http://localhost:8080/actuator/prometheus", "gateway_inflight_limit", {}))
    except Exception:
        return None


def gateway_published():
    try:
        return prom_counter("http://localhost:8080/actuator/prometheus", "gateway_usage_events_total", {"result": "published"})
    except Exception:
        return float("nan")


def gateway_failed():
    try:
        return prom_counter("http://localhost:8080/actuator/prometheus", "gateway_usage_events_total", {"result": "failed"})
    except Exception:
        return float("nan")


def consumer_lag():
    out = sh(["docker", "exec", "llmgw-kafka-1", "/opt/kafka/bin/kafka-consumer-groups.sh",
              "--bootstrap-server", "localhost:9092", "--describe", "--group", "metering"], timeout=60, check=False)
    lag = 0
    seen = False
    for line in out.splitlines():
        cols = line.split()
        if len(cols) >= 6 and cols[0] == "metering" and cols[1] == "usage-events":
            seen = True
            lag += int(cols[5]) if cols[5].isdigit() else 0
    return lag if seen else None


def wait_lag_zero(timeout=600):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if consumer_lag() == 0:
            return time.time() - t0
        time.sleep(1)
    raise RuntimeError("consumer lag did not drain")


def reconcile(k6_ok):
    time.sleep(3)
    wait_lag_zero()
    time.sleep(2)
    events = int(psql("select count(*) from usage_event"))
    distinct = int(psql("select count(distinct event_id) from usage_event"))
    rollup = int(psql("select coalesce(sum(requests),0) from usage_rollup_minute"))
    month = int(psql("select coalesce(sum(requests),0) from tenant_month_usage"))
    tokens_events = int(psql("select coalesce(sum(tokens_in + tokens_out),0) from usage_event"))
    tokens_month = int(psql("select coalesce(sum(tokens),0) from tenant_month_usage"))
    redis_tokens = 0
    keys = sh(["docker", "exec", "llmgw-redis-1", "redis-cli", "--scan", "--pattern", "quota:used:*"]).split()
    for k in keys:
        redis_tokens += int(sh(["docker", "exec", "llmgw-redis-1", "redis-cli", "get", k]).strip() or 0)
    return {
        "k6_ok_responses": k6_ok,
        "billed_events": events,
        "lost": k6_ok - events,
        "double_billed": events - distinct,
        "rollup_requests_match": rollup == events,
        "month_requests_match": month == events,
        "tokens_match": tokens_events == tokens_month == redis_tokens,
    }


def duplicates_absorbed():
    try:
        return prom_counter("http://localhost:8081/actuator/prometheus", "metering_events_total", {"result": "duplicate"})
    except Exception:
        return float("nan")


def power_source():
    if platform.system() != "Darwin":
        return "n/a"
    try:
        out = sh(["pmset", "-g", "batt"], timeout=10)
    except Exception:
        return "unknown"
    return "AC" if "AC Power" in out else "battery" if "Battery Power" in out else "unknown"


def require_ac_power():
    # On battery macOS throttles the CPU: a run on 2026-10-02 at 17% battery spent 2.4x the CPU per
    # request that the same image spent on AC, and nothing in the results showed why.
    if power_source() == "battery" and os.environ.get("ALLOW_BATTERY") != "1":
        sys.exit("refusing to benchmark on battery power (results are not comparable); plug in or set ALLOW_BATTERY=1")


def hardware():
    def q(cmd):
        try:
            return sh(cmd, timeout=20).strip()
        except Exception:
            return "?"
    return {
        "host": q(["sysctl", "-n", "machdep.cpu.brand_string"]) if platform.system() == "Darwin" else platform.processor(),
        "docker_cpus": q(["docker", "info", "--format", "{{.NCPU}}"]),
        "docker_mem_gb": round(int(q(["docker", "info", "--format", "{{.MemTotal}}"]) or 0) / 2**30, 1),
        "limits": f"gateway {os.environ.get('GATEWAY_CPUS', '2')} CPU / {os.environ.get('GATEWAY_MEM', '1g')}, "
                  "mock-upstream 3 CPU / 1.5 GB, metering 1 CPU / 768 MB (docker-compose.yml)",
        "trace_sampling": os.environ.get("TRACE_SAMPLE_RATIO", "see compose"),
        "host_load_avg_1m_at_record": round(os.getloadavg()[0], 2),
        "power": power_source(),
        "note": "laptop run; other processes were active, so treat these as one machine's numbers, not a capacity guarantee",
    }


def record(kind, payload):
    os.makedirs(os.path.dirname(SUMMARY), exist_ok=True)
    doc = {}
    if os.path.exists(SUMMARY):
        with open(SUMMARY) as f:
            doc = json.load(f)
    doc[kind] = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "hardware": hardware(), **payload}
    with open(SUMMARY, "w") as f:
        json.dump(doc, f, indent=2)
    print(json.dumps(doc[kind], indent=2))


def steps(rates, url=None, kind="steps", duration="45s"):
    env = {"DURATION": duration}
    if url:
        env["GATEWAY_URL"] = url
    print("warm-up 30s @ 1000 rps", flush=True)
    k6("step.js", f"{kind}-warmup", {**env, "RATE": 1000, "DURATION": "30s"})
    out = []
    for r in rates:
        res = {"target_rps": r, **k6("step.js", f"{kind}-{r}", {**env, "RATE": r})}
        res["holds"] = res["non2xx"] == 0 and res["dropped"] <= 0.001 * res["requests"] and res["p99_ms"] <= P99_SLO_MS
        out.append(res)
        if res["p99_ms"] > 4 * P99_SLO_MS or res["non2xx"] > 0.01 * res["requests"]:
            break
        time.sleep(10)
    held = [s for s in out if s["holds"]]
    best = max(held, key=lambda s: s["target_rps"]) if held else None
    record(kind, {"p99_slo_ms": P99_SLO_MS, "steps": out, "max_sustained": best})


def gateway_shed():
    try:
        return prom_counter("http://localhost:8080/actuator/prometheus", "gateway_requests_total", {"outcome": "shed"})
    except Exception:
        return float("nan")


def overload(rates, duration_s=45, recovery_rate=1000):
    """Offers more than the gateway can serve, then checks that a normal rate is served normally again."""
    out = []
    for r in rates:
        shed0 = gateway_shed()
        res = k6("step.js", f"overload-{r}", {"RATE": r, "DURATION": f"{duration_s}s"})
        res.update({"target_rps": r, "ok_rps": round(res["ok"] / duration_s, 1), "shed_503": int(gateway_shed() - shed0)})
        out.append(res)
        time.sleep(10)
    recovery = k6("step.js", "overload-recovery", {"RATE": recovery_rate, "DURATION": "30s"})
    recovery["target_rps"] = recovery_rate
    record("overload", {"max_in_flight": in_flight_limit(), "duration_s": duration_s,
                        "steps": out, "recovery": recovery})


SETTLED_LAG = 100


def chaos(kind, action_at, action, restore_after=None, restore=None, rate=1000, duration=150):
    # A cold JVM sheds for its first seconds while the JIT compiles (see "Cold start" in
    # docs/DESIGN.md). Without a warm-up those sheds land in the fault run and read as its cost.
    print(f"warm-up 30s @ {rate} rps", flush=True)
    k6("step.js", f"{kind}-warmup", {"RATE": rate, "DURATION": "30s"})
    time.sleep(5)
    reset_usage()
    published0 = gateway_published()
    failed0 = gateway_failed()
    lag_trace = []
    marks = {}
    stop = threading.Event()
    t0 = time.time()

    def sample():
        while not stop.is_set():
            lag_trace.append((round(time.time() - t0, 1), consumer_lag()))
            time.sleep(1)

    def inject():
        time.sleep(action_at)
        print(f"  t={time.time() - t0:.1f}s: {' '.join(action)}", flush=True)
        sh(action, timeout=180)
        if restore:
            time.sleep(restore_after)
            print(f"  t={time.time() - t0:.1f}s: {' '.join(restore)}", flush=True)
            sh(restore, timeout=180)
        marks["restored"] = round(time.time() - t0, 1)
        print(f"  t={marks['restored']}s: service back", flush=True)

    sampler = threading.Thread(target=sample, daemon=True)
    injector = threading.Thread(target=inject, daemon=True)
    sampler.start()
    injector.start()
    res = k6("step.js", kind, {"RATE": rate, "DURATION": f"{duration}s"}, timeout=duration + 600)
    injector.join()
    k6_end = round(time.time() - t0, 1)
    rec = reconcile(res["ok"])
    stop.set()
    restored = marks["restored"]
    # Under steady load the lag sits at a few dozen events and is rarely exactly 0, so "caught up"
    # means back under SETTLED_LAG (0.1 s of traffic at 1000 requests/s).
    drained = next((t for t, lag in lag_trace if t >= restored and lag is not None and lag < SETTLED_LAG), None)
    peak = max((l for _, l in lag_trace if l is not None), default=None)
    record(kind, {
        "rate": rate,
        "duration_s": duration,
        "fault": f"{' '.join(action)} at t={action_at}s" + (f", then {' '.join(restore)} {restore_after}s later" if restore else ""),
        "service_back_at_s": restored,
        "k6_finished_at_s": k6_end,
        "k6": res,
        "peak_consumer_lag": peak,
        "lag_settled_after_service_back_s": round(drained - restored, 1) if drained is not None else None,
        "lag_settled_below": SETTLED_LAG,
        "lag_sample_interval": "about 1-2 s (kafka-consumer-groups.sh per sample)",
        "gateway_events_published": gateway_published() - published0,
        "gateway_events_failed": gateway_failed() - failed0,
        "metering_duplicates_absorbed": duplicates_absorbed(),
        "metering_duplicates_note": "counter of the metering process alive at the end of the run",
        "reconciliation": rec,
        "lag_trace": lag_trace,
    })



def to_mib(text):
    m = re.match(r"([\d.]+)\s*([KMG]i?B|B)", text.strip())
    if not m:
        return 0.0
    scale = {"B": 1 / 2**20, "KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}[m.group(2)]
    return float(m.group(1)) * scale


def container_stats():
    """CPU in cores and memory in MiB for each compose container, from one `docker stats` sample."""
    out = sh(["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"], timeout=60)
    stats = {}
    for line in out.splitlines():
        name, cpu, mem = line.split("|")
        if name.startswith("llmgw-"):
            key = name.removeprefix("llmgw-").removesuffix("-1")
            stats[key] = {"cpu": float(cpu.rstrip("%") or 0) / 100, "mem_mib": to_mib(mem.split("/")[0])}
    return stats


class StatsSampler:
    """Samples `docker stats` in the background (one sample takes about 2 s)."""

    def __init__(self):
        self.samples, self.stop, self.t0 = [], threading.Event(), time.time()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self.stop.is_set():
            try:
                self.samples.append((time.time() - self.t0, container_stats()))
            except Exception:
                time.sleep(1)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join()

    def usage(self, start_s=0, end_s=None):
        use = [s for t, s in self.samples if t >= start_s and (end_s is None or t <= end_s)] or [s for _, s in self.samples]
        names = sorted(set().union(*use)) if use else []
        return {n: {"cpu_cores": round(sum(s[n]["cpu"] for s in use if n in s) / max(1, sum(n in s for s in use)), 2),
                    "mem_mib_max": round(max(s[n]["mem_mib"] for s in use if n in s))} for n in names}


def capacity(rates, duration_s=45):
    """Offers each rate while sampling every container, so a run says both what the gateway served
    and what each component spent per request. CPU-ms per request is cores x 1000 / served rps."""
    cpus = os.environ.get("GATEWAY_CPUS", "2")
    limit = in_flight_limit()
    # The in-flight limit caps throughput at limit / mean latency (Little's law), so a run with an
    # explicit MAX_IN_FLIGHT is recorded separately from the default (128 per CPU).
    name = f"capacity-{cpus}cpu" + (f"-inflight{limit}" if int(os.environ.get("MAX_IN_FLIGHT", "0")) else "")
    # A comparison run (say, two gateway versions back to back) must not overwrite the reference run.
    if os.environ.get("TAG"):
        name += f"-{os.environ['TAG']}"
    print("warm-up 30s @ 1000 rps", flush=True)
    k6("step.js", f"{name}-warmup", {"RATE": 1000, "DURATION": "30s"})
    out = []
    for r in rates:
        time.sleep(10)
        with StatsSampler() as st:
            res = k6("step.js", f"{name}-{r}", {"RATE": r, "DURATION": f"{duration_s}s"})
        # skip k6 start-up and the first seconds of the step, stop before it winds down
        usage = st.usage(start_s=10, end_s=duration_s)
        served = res["ok"] / duration_s
        res.update({"target_rps": r, "ok_rps": round(served, 1), "usage": usage,
                    "cpu_ms_per_request": {n: round(u["cpu_cores"] * 1000 / served, 3) for n, u in usage.items()} if served else {}})
        out.append(res)
    record(name, {"gateway_cpus": float(cpus), "max_in_flight": limit, "duration_s": duration_s, "steps": out})


def drain(rate=2000, duration_s=90):
    """Stops metering, builds a backlog in Kafka, then starts metering and times how fast the
    backlog becomes billing rows in Postgres."""
    reset_usage()
    sh(["docker", "stop", "llmgw-metering-1"], timeout=120)
    res = k6("step.js", "drain-backlog", {"RATE": rate, "DURATION": f"{duration_s}s"})
    time.sleep(3)
    backlog = consumer_lag()
    print(f"  backlog {backlog} events, starting metering", flush=True)
    trace = []
    with StatsSampler() as st:
        sh(["docker", "start", "llmgw-metering-1"])
        t0 = time.time()
        while time.time() - t0 < 600:
            n = int(psql("select count(*) from usage_event"))
            trace.append((round(time.time() - t0, 2), n))
            if n >= res["ok"]:
                break
            time.sleep(0.3)
    first = next(((t, n) for t, n in trace if n > 0), None)
    near_end = next(((t, n) for t, n in trace if n >= 0.95 * res["ok"]), None)
    rate_eps = round((near_end[1] - first[1]) / (near_end[0] - first[0])) if first and near_end and near_end[0] > first[0] else None
    busy = st.usage(start_s=(st.samples[0][0] + first[0]) if first and st.samples else 0)
    rec = reconcile(res["ok"])
    record("drain", {
        "backlog_events": backlog, "k6": res, "metering_cpu_limit": 1,
        "first_row_after_start_s": first[0] if first else None,
        "drain_events_per_s": rate_eps,
        "drain_rate_window": "from the first billed row to 95% of the backlog, by count(*) on usage_event",
        "usage_while_draining": {k: v for k, v in busy.items() if k in ("metering", "postgres", "kafka", "redis")},
        "reconciliation": rec, "trace": trace[::5],
    })


def streams(counts, ramp_s=20):
    """Many long streams open at once, read by loadtest/Streams.java (virtual threads; k6 would
    need a VU per open stream). Run it against the slow mock with the limit raised, see README."""
    hold_ms = int(os.environ.get("MOCK_MEDIAN_MS", "30000"))
    out = []
    for n in counts:
        reset_usage()
        cmd = ["docker", "run", "--rm", "--network", "llmgw_default", "-v", f"{ROOT}/loadtest:/lt:ro",
               "eclipse-temurin:21-jdk", "java", "-Xmx2g", "/lt/Streams.java", "http://gateway:8080", str(n), str(ramp_s)]
        with StatsSampler() as st:
            text = sh(cmd, timeout=ramp_s + hold_ms / 1000 + 600)
        line = next(l for l in text.splitlines() if l.startswith("STREAMS_JSON "))
        r = json.loads(line.split(" ", 1)[1])
        gw = st.usage().get("gateway", {})
        idle = min((s["gateway"]["mem_mib"] for _, s in st.samples[:2] if "gateway" in s), default=None)
        r["gateway"] = {"mem_mib_max": gw.get("mem_mib_max"), "mem_mib_at_start": round(idle) if idle else None,
                        "cpu_cores_mean": gw.get("cpu_cores")}
        r["reconciliation"] = reconcile(r["completed"])
        # 499: the client went away first; 502: the upstream stream broke. Both are still billed.
        r["billed_by_status"] = {int(a): int(b) for a, b in (l.split("|") for l in
                                 psql("select status, count(*) from usage_event group by status").splitlines() if l)}
        mock = st.usage().get("mock-upstream", {})
        r["mock_upstream_mem_mib_max"] = mock.get("mem_mib_max")
        print(f"  streams {n}: completed={r['completed']} failed={r['failed']} peak_open={r['peak_open']} "
              f"first_event_p99={r['first_event_ms'] and r['first_event_ms']['p99']}ms gateway_mem_max={gw.get('mem_mib_max')}MiB "
              f"lost={r['reconciliation']['lost']} by_status={r['billed_by_status']} mock_mem_max={r['mock_upstream_mem_mib_max']}MiB", flush=True)
        out.append(r)
        if r["failed"] > 0.01 * n:
            r["gateway_after_failure"] = gateway_state()
            print(f"  gateway after failure: {r['gateway_after_failure']}", flush=True)
            break
        time.sleep(10)
    mem = os.environ.get("GATEWAY_MEM", "1g")
    record("streams" + ("" if mem == "1g" else f"-{mem}"), {
        "upstream_stream_ms": hold_ms, "upstream_chunks": int(os.environ.get("MOCK_STREAM_CHUNKS", "8")),
        "ramp_s": ramp_s, "max_in_flight": in_flight_limit(), "gateway_mem_limit": mem,
        "steps": out})


RACE_TENANT = "race"


def race_tenant(quota):
    psql(f"insert into tenant (id, name, rate_per_sec, burst, monthly_token_quota) values "
         f"('{RACE_TENANT}', 'Quota race', 100000, 200000, {quota}) "
         f"on conflict (id) do update set monthly_token_quota = excluded.monthly_token_quota, "
         f"rate_per_sec = excluded.rate_per_sec, burst = excluded.burst")
    psql(f"insert into api_key (key_hash, tenant_id, label) values "
         f"(encode(sha256(convert_to('sk-dev-{RACE_TENANT}', 'UTF8')), 'hex'), '{RACE_TENANT}', 'dev') "
         f"on conflict (key_hash) do nothing")
    psql(f"delete from usage_event where tenant_id = '{RACE_TENANT}'")
    psql(f"delete from usage_rollup_minute where tenant_id = '{RACE_TENANT}'")
    psql(f"delete from tenant_month_usage where tenant_id = '{RACE_TENANT}'")
    sh(["docker", "exec", "llmgw-redis-1", "sh", "-c",
        f"redis-cli --scan --pattern '*{{{RACE_TENANT}}}*' | xargs -r redis-cli del >/dev/null"])


def quota_race(rates, quota, duration_s=10, version="current"):
    """A tenant with a fixed monthly token quota is hit at each rate until it runs out.

    The quota is a billing control, so the number that matters is how many tokens were billed past
    it. Every figure is read back from Postgres after the consumer lag reaches zero.
    """
    print("warm-up 30s @ 1000 rps", flush=True)
    k6("step.js", "quota-race-warmup", {"RATE": 1000, "DURATION": "30s"})
    runs = []
    for rate in rates:
        race_tenant(quota)
        time.sleep(2)
        res = k6("step.js", f"quota-race-{version}-{rate}",
                 {"RATE": rate, "DURATION": f"{duration_s}s", "TENANT": RACE_TENANT})
        time.sleep(3)
        wait_lag_zero()
        time.sleep(2)
        billed = int(psql(f"select coalesce(sum(tokens),0) from tenant_month_usage where tenant_id = '{RACE_TENANT}'") or 0)
        rows = int(psql(f"select count(*) from usage_event where tenant_id = '{RACE_TENANT}'"))
        ok_rows = int(psql(f"select count(*) from usage_event where tenant_id = '{RACE_TENANT}' and status = 200"))
        out = {
            "version": version,
            "rate": rate,
            "duration_s": duration_s,
            "quota_tokens": quota,
            "billed_tokens": billed,
            "overshoot_tokens": billed - quota,
            "overshoot_pct": round(100.0 * (billed - quota) / quota, 2),
            "billed_requests": rows,
            "billed_ok_requests": ok_rows,
            "k6": res,
        }
        print(f"  quota {quota}: billed {billed} ({out['overshoot_pct']:+}%), {ok_rows} requests served", flush=True)
        runs.append(out)
        record(f"quota-race-{version}", {"runs": runs})


MOCK = "llmgw-mock-upstream-1"


def mock(path, method="GET"):
    return sh(["docker", "exec", MOCK, "curl", "-fsS", "-X", method, f"http://localhost:8090{path}"], timeout=120)


def open_leases():
    """Leases not yet released plus reaped records not yet acknowledged by Kafka, across all tenants."""
    out = sh(["docker", "exec", "llmgw-redis-1", "sh", "-c",
              "for k in $(redis-cli --scan --pattern 'lease:{*'); do redis-cli hlen \"$k\"; done; "
              "for k in $(redis-cli --scan --pattern 'lease:reaped:*'); do redis-cli llen \"$k\"; done"])
    return sum(int(x) for x in out.split() if x.strip().isdigit())


def wait_healthy(container, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        state = sh(["docker", "inspect", container, "--format", "{{.State.Health.Status}}"], check=False).strip()
        if state == "healthy":
            return time.time() - t0
        time.sleep(1)
    raise RuntimeError(f"{container} did not become healthy")


def gateway_kill(rate=1000, duration=60, kill_at=20, down_s=5, version="current"):
    """SIGKILL the gateway under load and account for every request the provider served.

    A killed gateway cannot publish anything, so what this measures is what the design recovers
    afterwards. The mock provider records the X-Request-Id of every request it served (the gateway
    sends its lease id there); after the leases of the killed requests expire and are reaped, each
    served request is looked up in Postgres. A gateway that sends no request id (v1) is reconciled by
    counts instead.
    """
    print(f"warm-up 30s @ {rate} rps", flush=True)
    k6("step.js", "gateway-kill-warmup", {"RATE": rate, "DURATION": "30s"})
    wait_lag_zero()
    time.sleep(5)
    reset_usage()
    mock("/stats/reset?record=true", "POST")
    marks = {}
    t0 = time.time()

    def inject():
        time.sleep(kill_at)
        marks["killed_at_s"] = round(time.time() - t0, 1)
        print(f"  t={marks['killed_at_s']}s: SIGKILL gateway", flush=True)
        sh(["docker", "kill", "-s", "KILL", "llmgw-gateway-1"])
        time.sleep(down_s)
        sh(["docker", "start", "llmgw-gateway-1"])
        wait_healthy("llmgw-gateway-1")
        marks["healthy_at_s"] = round(time.time() - t0, 1)
        print(f"  t={marks['healthy_at_s']}s: gateway healthy again", flush=True)

    injector = threading.Thread(target=inject, daemon=True)
    injector.start()
    res = k6("step.js", f"gateway-kill-{version}", {"RATE": rate, "DURATION": f"{duration}s"}, timeout=duration + 600)
    injector.join()
    k6_end = time.time() - t0

    # The killed requests' leases expire LEASE_TTL after admission; the reaper then bills them.
    print("  waiting for every lease to be reaped and billed...", flush=True)
    t1 = time.time()
    while True:
        leases = open_leases()
        if leases == 0 and consumer_lag() == 0:
            break
        if time.time() - t1 > 900:
            raise RuntimeError(f"{leases} leases still open after 15 minutes")
        time.sleep(2)
    time.sleep(3)
    settled_after_s = round(time.time() - t0, 1)

    served = json.loads(mock("/stats/served"))
    ids = {}
    for line in mock("/stats/request-ids").splitlines():
        rid, p, c = line.split()
        ids[rid] = (int(p), int(c))
    billed = {}
    for line in psql("select event_id, tokens_in, tokens_out, status, usage_source from usage_event").splitlines():
        eid, tin, tout, status, source = line.split("|")
        billed[eid] = (int(tin), int(tout), int(status), source)
    with_tokens = [k for k, v in billed.items() if v[0] + v[1] > 0]
    out = {
        "version": version,
        "rate": rate,
        "duration_s": duration,
        "fault": f"docker kill -s KILL llmgw-gateway-1 at t={kill_at}s, docker start {down_s}s later",
        **marks,
        "k6_finished_at_s": round(k6_end, 1),
        "all_billed_at_s": settled_after_s,
        "k6": res,
        "provider_served_requests": served["requests"],
        "provider_served_tokens": served["tokens"],
        "billed_rows": len(billed),
        "billed_rows_with_tokens": len(with_tokens),
        "billed_tokens": sum(v[0] + v[1] for v in billed.values()),
        "unbilled_served_requests_by_count": served["requests"] - len(with_tokens),
    }
    if ids:
        expired = [k for k, v in billed.items() if v[3] == "lease_expired"]
        expired_served = [k for k in expired if k in ids]
        provider_rows = [k for k, v in billed.items() if v[3] == "provider" and k in ids]
        out.update({
            "served_ids_recorded": len(ids),
            "served_but_not_billed": sum(1 for k in ids if k not in billed),
            "billed_with_tokens_but_never_served": sum(1 for k in with_tokens if k not in ids),
            "provider_rows_matching_exactly": sum(1 for k in provider_rows if billed[k][:2] == ids[k]),
            "provider_rows": len(provider_rows),
            "reaped_leases_billed_at_reservation": len(expired),
            "reaped_leases_the_provider_had_served": len(expired_served),
            "reaped_overbilled_tokens": sum(billed[k][0] + billed[k][1] - sum(ids[k]) for k in expired_served),
            "estimated_rows": sum(1 for v in billed.values() if v[3] == "estimated"),
        })
    print(json.dumps({k: v for k, v in out.items() if k != "k6"}, indent=2), flush=True)
    record(f"gateway-kill-{version}", out)
    mock("/stats/reset?record=false", "POST")


REAL_PROMPTS = [
    [{"role": "user", "content": "hi"}],
    [{"role": "user", "content": "Summarise last week of campaign performance in three bullet points."}],
    [{"role": "system", "content": "You are a terse assistant. Answer in one sentence."},
     {"role": "user", "content": "Why do idempotent consumers matter in event-driven billing?"}],
    [{"role": "user", "content": "Write a Python function that merges two sorted lists, with a docstring and tests."}],
    [{"role": "user", "content": "Rewrite as a formal email: the October invoice is ready and will be paid in five days."}],
    [{"role": "user", "content": "In three sentences, explain what a monthly token quota is."}],
    [{"role": "user", "content": "Explain the difference between at-least-once and exactly-once delivery. " * 6}],
    [{"role": "system", "content": "You review SQL."},
     {"role": "user", "content": "select tenant_id, sum(tokens) from usage_event where occurred_at > now() - interval '1 day' group by 1;"},
     {"role": "assistant", "content": "The query is fine, but it needs an index on occurred_at."},
     {"role": "user", "content": "Which index exactly, and would a BRIN index work here?"}],
    [{"role": "user", "content": "List ten JSON field names for a usage event, as a JSON array only."}],
    [{"role": "user", "content": "Tell me a two-line story about a rate limiter that learned to say no."}],
]


def gateway_post(key, body, timeout=180):
    req = urllib.request.Request("http://localhost:8080/v1/chat/completions", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode(), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(), dict(e.headers)


def tenant_with_key(tid, quota, rate=100000):
    psql(f"insert into tenant (id, name, rate_per_sec, burst, monthly_token_quota) values "
         f"('{tid}', '{tid}', {rate}, {rate * 2}, {quota}) on conflict (id) do update set "
         f"monthly_token_quota = excluded.monthly_token_quota")
    psql(f"insert into api_key (key_hash, tenant_id, label) values "
         f"(encode(sha256(convert_to('sk-dev-{tid}', 'UTF8')), 'hex'), '{tid}', 'dev') on conflict (key_hash) do nothing")
    for t in ("usage_event", "usage_rollup_minute", "tenant_month_usage"):
        psql(f"delete from {t} where tenant_id = '{tid}'")
    sh(["docker", "exec", "llmgw-redis-1", "sh", "-c",
        f"redis-cli --scan --pattern '*{{{tid}}}*' | xargs -r redis-cli del >/dev/null"])
    return f"sk-dev-{tid}"


def pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1) if xs else None


def real_model(model, n=200, concurrency=4, quota=3000, race_requests=60):
    """Needs the gateway pointed at Ollama with every output counted:
    UPSTREAM_BASE_URL=http://host.docker.internal:11434 ESTIMATE_SAMPLE_RATE=1.0 docker compose up -d gateway
    """
    import random
    from concurrent.futures import ThreadPoolExecutor
    rnd = random.Random(7)
    key = tenant_with_key("real", 10**12)
    print(f"{n} requests to {model}, {concurrency} at a time", flush=True)

    def one(i):
        body = {"model": model, "messages": REAL_PROMPTS[i % len(REAL_PROMPTS)], "max_tokens": rnd.choice([16, 64, 128, 256]),
                "stream": i % 2 == 1}
        status, _, _ = gateway_post(key, body)
        return status

    t0 = time.time()
    with ThreadPoolExecutor(concurrency) as pool:
        statuses = list(pool.map(one, range(n)))
    took = time.time() - t0
    wait_lag_zero()
    time.sleep(2)
    rows = [l.split("|") for l in psql(
        "select tokens_in, tokens_out, coalesce(est_tokens_in, -1), coalesce(est_tokens_out, -1), usage_source, streamed "
        "from usage_event where tenant_id = 'real' and status = 200").splitlines()]
    prompt_err, out_err, sources = [], [], {}
    for tin, tout, ein, eout, src, _ in rows:
        tin, tout, ein, eout = int(tin), int(tout), int(ein), int(eout)
        sources[src] = sources.get(src, 0) + 1
        if src == "provider" and tin > 0 and ein >= 0:
            prompt_err.append(100.0 * (ein - tin) / tin)
        if src == "provider" and tout > 0 and eout >= 0:
            out_err.append(100.0 * (eout - tout) / tout)
    calib = None
    try:
        calib = prom_counter("http://localhost:8080/actuator/prometheus", "gateway_token_prompt_excess", {"model": model})
    except Exception:
        pass
    drift = {
        "requests": n, "ok": statuses.count(200), "seconds": round(took, 1), "billed_rows": len(rows), "usage_sources": sources,
        "prompt_estimate_error_pct": {"p50": pct(prompt_err, .5), "p10": pct(prompt_err, .1), "p90": pct(prompt_err, .9),
                                      "min": pct(prompt_err, 0), "max": pct(prompt_err, 1)},
        "output_estimate_error_pct": {"p50": pct(out_err, .5), "p10": pct(out_err, .1), "p90": pct(out_err, .9),
                                      "min": pct(out_err, 0), "max": pct(out_err, 1)},
        "learned_prompt_excess_tokens": calib,
        "note": "error = (gateway count - model's reported count) / model's count; negative = gateway counts fewer",
    }
    print(json.dumps(drift, indent=2), flush=True)

    # Quota race against the real model: every request may generate up to 200 tokens.
    rkey = tenant_with_key("real-quota", quota)
    print(f"quota race: {race_requests} requests at once against a {quota}-token quota (retrying while reserved)", flush=True)

    def race(i):
        # Like a real client: retry while the quota is only reserved, stop once it is exhausted.
        body = {"model": model, "messages": REAL_PROMPTS[i % len(REAL_PROMPTS)], "max_tokens": 200, "stream": i % 3 == 0}
        retries = 0
        while True:
            status, text, headers = gateway_post(rkey, body)
            if status == 429 and "quota_reserved" in text and retries < 120:
                retries += 1
                time.sleep(0.5)
                continue
            return status, "insufficient_quota" in text, headers.get("X-Granted-Max-Tokens"), retries

    with ThreadPoolExecutor(race_requests) as pool:
        results = list(pool.map(race, range(race_requests)))
    wait_lag_zero()
    time.sleep(2)
    billed = int(psql("select coalesce(sum(tokens),0) from tenant_month_usage where tenant_id = 'real-quota'") or 0)
    race_out = {
        "quota_tokens": quota, "requests": race_requests,
        "served": sum(1 for s, _, _, _ in results if s == 200),
        "refused_quota": sum(1 for s, q, _, _ in results if s == 429 and q),
        "served_with_reduced_max_tokens": sum(1 for s, _, g, _ in results if s == 200 and g),
        "retries_while_reserved": sum(r for _, _, _, r in results),
        "billed_tokens": billed, "overshoot_tokens": billed - quota, "overshoot_pct": round(100.0 * (billed - quota) / quota, 2),
    }
    print(json.dumps(race_out, indent=2), flush=True)
    record(f"real-model-{model}", {"model": model, "drift": drift, "quota_race": race_out})


def gateway_state():
    """Whether the gateway container survived, and the last error lines it logged."""
    fmt = "{{.State.Status}} {{.State.OOMKilled}} {{.State.ExitCode}} {{.RestartCount}}"
    status, oom, code, restarts = sh(["docker", "inspect", "llmgw-gateway-1", "--format", fmt]).split()
    logs = subprocess.run(["docker", "logs", "--tail", "400", "llmgw-gateway-1"], capture_output=True, text=True)
    errors = [l[:300] for l in (logs.stdout + logs.stderr).splitlines()
              if re.search(r"OutOfMemory|Exception|Error|Killed|terminating", l)]
    return {"status": status, "oom_killed": oom == "true", "exit_code": int(code), "restarts": int(restarts),
            "last_errors": errors[-8:]}


def main():
    os.makedirs(RAW, exist_ok=True)
    what = sys.argv[1] if len(sys.argv) > 1 else "steps"
    if what != "smoke":  # smoke checks correctness only, so power state does not matter
        require_ac_power()
    rates = [int(x) for x in os.environ.get("RATES", "1000,2000,3000,4000,5000,6000,7000,8000").split(",")]
    if what == "steps":
        steps(rates)
    elif what == "overload":
        overload([int(x) for x in os.environ.get("RATES", "6000,8000").split(",")])
    elif what == "baseline":
        steps(rates, url="http://mock-upstream:8090", kind="baseline")
    elif what == "smoke":
        # Warm up first: a cold JVM sheds while the JIT compiles, which is not what smoke checks.
        k6("step.js", "smoke-warmup", {"RATE": rates[0], "DURATION": "30s"})
        wait_lag_zero()
        reset_usage()
        res = k6("step.js", "smoke", {"RATE": rates[0], "DURATION": "20s"})
        rec = reconcile(res["ok"])
        print(json.dumps({"k6": res, "reconciliation": rec}, indent=2))
        bad = res["non2xx"] or rec["lost"] or rec["double_billed"] or not (
            rec["rollup_requests_match"] and rec["month_requests_match"] and rec["tokens_match"])
        sys.exit(1 if bad else 0)
    elif what == "burst":
        res = k6("burst.js", "burst", {"BASE_RATE": os.environ.get("BASE_RATE", 300), "PEAK_RATE": os.environ.get("PEAK_RATE", 3000)})
        record("burst", {"base_rps": int(os.environ.get("BASE_RATE", 300)), "peak_rps": int(os.environ.get("PEAK_RATE", 3000)), "k6": res})
    elif what == "capacity":
        capacity([int(x) for x in os.environ.get("RATES", "2000,4000,6000").split(",")])
    elif what == "drain":
        drain(int(os.environ.get("RATE", "2000")), int(os.environ.get("DURATION_S", "90")))
    elif what == "streams":
        streams([int(x) for x in os.environ.get("COUNTS", "2000,5000,10000").split(",")], int(os.environ.get("RAMP_S", "20")))
    elif what == "quota-race":
        quota_race([int(x) for x in os.environ.get("RATES", "500,2000,5000").split(",")],
                   int(os.environ.get("QUOTA", "300000")), int(os.environ.get("DURATION_S", "10")),
                   os.environ.get("VERSION", "current"))
    elif what == "real-model":
        real_model(os.environ.get("MODEL", "qwen2.5:0.5b"), int(os.environ.get("N", "200")),
                   int(os.environ.get("CONCURRENCY", "4")), int(os.environ.get("QUOTA", "3000")))
    elif what == "gateway-kill":
        gateway_kill(int(os.environ.get("RATE", "1000")), int(os.environ.get("DURATION_S", "60")),
                     version=os.environ.get("VERSION", "current"))
    elif what == "consumer-kill":
        chaos("consumer-kill", 30, ["docker", "kill", "-s", "KILL", "llmgw-metering-1"], 45, ["docker", "start", "llmgw-metering-1"])
    elif what == "broker-restart":
        chaos("broker-restart", 30, ["docker", "restart", "llmgw-kafka-1"])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
