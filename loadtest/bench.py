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
    out = sh(args, timeout=timeout)
    line = next((l for l in out.splitlines() if l.startswith(name + ":")), out.strip()[-300:])
    print("  " + line, flush=True)
    with open(os.path.join(RAW, name + ".json")) as f:
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
        "redis-cli --scan --pattern 'quota:*' | xargs -r redis-cli del >/dev/null"])


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
    keys = sh(["docker", "exec", "llmgw-redis-1", "redis-cli", "--scan", "--pattern", "quota:*"]).split()
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
    # On battery macOS throttles the CPU: a run on 2026-10-02 at 17% battery served 1,300 requests/s
    # where the same image had served 5,000 on AC, and nothing in the results showed why.
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
    require_ac_power()
    rates = [int(x) for x in os.environ.get("RATES", "1000,2000,3000,4000,5000,6000,7000,8000").split(",")]
    if what == "steps":
        steps(rates)
    elif what == "overload":
        overload([int(x) for x in os.environ.get("RATES", "6000,8000").split(",")])
    elif what == "baseline":
        steps(rates, url="http://mock-upstream:8090", kind="baseline")
    elif what == "smoke":
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
    elif what == "consumer-kill":
        chaos("consumer-kill", 30, ["docker", "kill", "-s", "KILL", "llmgw-metering-1"], 45, ["docker", "start", "llmgw-metering-1"])
    elif what == "broker-restart":
        chaos("broker-restart", 30, ["docker", "restart", "llmgw-kafka-1"])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
