#!/usr/bin/env python3
"""Renders results/summary.json into results/index.html (self-contained, no external assets)."""
import html
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))


def esc(x):
    return html.escape(str(x))


def latency_chart(steps, baseline):
    w, h, pad = 640, 260, 46
    pts = [s for s in steps if s.get("p99_ms") is not None]
    if not pts:
        return ""
    xmax = max(s["target_rps"] for s in pts) * 1.05
    ymax = max(max(s["p99_ms"] for s in pts), 50) * 1.1
    if baseline:
        ymax = max(ymax, max(s["p99_ms"] for s in baseline) * 1.1)

    def xy(s, key):
        x = pad + (s["target_rps"] / xmax) * (w - pad - 12)
        y = h - pad + 8 - (s[key] / ymax) * (h - pad - 16)
        return x, y

    def line(series, key, cls):
        p = " ".join(f"{x:.1f},{y:.1f}" for x, y in (xy(s, key) for s in series))
        dots = "".join(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" class="{cls}"/>' for x, y in (xy(s, key) for s in series))
        return f'<polyline points="{p}" class="{cls}" fill="none"/>{dots}'

    grid = ""
    for i in range(5):
        v = ymax * i / 4
        y = h - pad + 8 - (v / ymax) * (h - pad - 16)
        grid += f'<line x1="{pad}" x2="{w - 12}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/><text x="{pad - 6}" y="{y + 4:.1f}" text-anchor="end">{v:.0f}</text>'
    for s in pts:
        x, _ = xy(s, "p99_ms")
        grid += f'<text x="{x:.1f}" y="{h - pad + 24}" text-anchor="middle">{s["target_rps"]}</text>'
    body = grid + line(pts, "p50_ms", "p50") + line(pts, "p99_ms", "p99")
    if baseline:
        body += line(baseline, "p99_ms", "base")
    return (f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="Latency against offered load">{body}'
            f'<text x="{w / 2}" y="{h - 4}" text-anchor="middle">offered load (requests/s)</text>'
            f'<text x="12" y="{h / 2}" transform="rotate(-90 12 {h / 2})" text-anchor="middle">ms</text></svg>')


def table(rows, cols):
    head = "".join(f"<th>{esc(c[0])}</th>" for c in cols)
    body = ""
    for r in rows:
        body += "<tr>" + "".join(f"<td>{esc(c[1](r))}</td>" for c in cols) + "</tr>"
    return f'<div class="tw"><table><tr>{head}</tr>{body}</table></div>'


def main():
    with open(os.path.join(HERE, "summary.json")) as f:
        doc = json.load(f)
    steps = doc.get("steps", {})
    base = doc.get("baseline", {})
    parts = []
    hw = steps.get("hardware") or next((v.get("hardware") for v in doc.values() if isinstance(v, dict)), {})
    parts.append(f'<p class="sub">Measured {esc(steps.get("at", ""))} on {esc(hw.get("host", "?"))}, Docker with '
                 f'{esc(hw.get("docker_cpus", "?"))} CPUs and {esc(hw.get("docker_mem_gb", "?"))} GB. '
                 f'{esc(hw.get("limits", ""))}. Trace sampling {esc(hw.get("trace_sampling", "?"))}. '
                 f'{esc(hw.get("note", ""))}</p>')

    best = steps.get("max_sustained")
    if best:
        parts.append(f'<div class="card"><b>Max sustained load</b> (no errors, no dropped iterations, p99 at or under '
                     f'{esc(steps.get("p99_slo_ms"))} ms): <b>{esc(best["target_rps"])} requests/s</b>, '
                     f'p50 {esc(best["p50_ms"])} ms, p95 {esc(best["p95_ms"])} ms, p99 {esc(best["p99_ms"])} ms, '
                     f'end to end through the gateway, with the mock upstream itself taking about 40 ms at the median.</div>')

    if steps.get("steps"):
        parts.append("<h2>Saturation steps</h2>")
        parts.append('<p class="legend"><span class="k p50"></span>gateway p50 <span class="k p99"></span>gateway p99 '
                     '<span class="k base"></span>upstream alone p99</p>')
        parts.append(latency_chart(steps["steps"], base.get("steps")))
        cols = [("offered rps", lambda r: r["target_rps"]), ("achieved rps", lambda r: r["rps"]),
                ("requests", lambda r: r["requests"]), ("non-2xx", lambda r: r["non2xx"]),
                ("dropped", lambda r: r["dropped"]), ("p50 ms", lambda r: r["p50_ms"]),
                ("p95 ms", lambda r: r["p95_ms"]), ("p99 ms", lambda r: r["p99_ms"]),
                ("p99 of 2xx ms", lambda r: r.get("ok_p99_ms", "")),
                ("max ms", lambda r: r["max_ms"]), ("holds", lambda r: "yes" if r["holds"] else "no")]
        parts.append(table(steps["steps"], cols))

    if "overload" in doc:
        o = doc["overload"]
        parts.append(f'<h2>Above capacity</h2><p>Offered load past saturation for {esc(o["duration_s"])} s per rate, with '
                     f'max-in-flight {esc(o["max_in_flight"])}. The gateway answers the excess with an immediate 503 and '
                     f'<code>Retry-After: 1</code>; the "p99 of 2xx" column is the latency of the requests it admitted. '
                     f'A {esc(o["recovery"]["target_rps"])} requests/s step follows to show it recovers.</p>')
        rows = o["steps"] + [{**o["recovery"], "ok_rps": round(o["recovery"]["ok"] / 30, 1), "shed_503": "", "target_rps": f'{o["recovery"]["target_rps"]} (recovery)'}]
        parts.append(table(rows, [("offered rps", lambda r: r["target_rps"]), ("served rps", lambda r: r["ok_rps"]),
                                  ("shed (503)", lambda r: r["shed_503"]), ("non-2xx", lambda r: r["non2xx"]),
                                  ("dropped", lambda r: r["dropped"]), ("p99 of 2xx ms", lambda r: r["ok_p99_ms"]),
                                  ("p99 all ms", lambda r: r["p99_ms"]), ("max ms", lambda r: r["max_ms"])]))

    tuning = [(k, doc[k]) for k in ("steps-serialgc", "steps-g1-uncapped", "steps-limit512", "steps-limit256") if k in doc]
    if tuning:
        parts.append("<h2>How the configuration got here</h2><p>Earlier runs of the same steps, before each fix in "
                     "<code>docs/DESIGN.md</code>. A row stops where that run's early-stop rule ended it.</p>")
        rows = []
        for k, v in tuning + ([("steps (final)", steps)] if steps.get("steps") else []):
            for st in v["steps"]:
                rows.append({"cfg": v.get("jvm", k), "rps": st["target_rps"], "bad": st["non2xx"], "drop": st["dropped"],
                             "p99": st["p99_ms"], "okp99": st.get("ok_p99_ms", ""), "max": st["max_ms"]})
        parts.append(table(rows, [("configuration", lambda r: r["cfg"]), ("offered rps", lambda r: r["rps"]),
                                  ("non-2xx", lambda r: r["bad"]), ("dropped", lambda r: r["drop"]),
                                  ("p99 ms", lambda r: r["p99"]), ("p99 of 2xx ms", lambda r: r["okp99"]),
                                  ("max ms", lambda r: r["max"])]))

    if base.get("steps"):
        parts.append("<h2>Gateway overhead</h2><p>The same steps sent straight to the mock upstream. The difference is "
                     "what the gateway adds: auth, the admission script, the upstream hop and the usage event.</p>")
        by_rate = {s["target_rps"]: s for s in base["steps"]}
        rows = []
        for s in steps.get("steps", []):
            b = by_rate.get(s["target_rps"])
            if b:
                rows.append({"rps": s["target_rps"], "gp50": s["p50_ms"], "bp50": b["p50_ms"],
                             "gp99": s["p99_ms"], "bp99": b["p99_ms"]})
        parts.append(table(rows, [("rps", lambda r: r["rps"]), ("upstream p50", lambda r: r["bp50"]),
                                  ("gateway p50", lambda r: r["gp50"]), ("added p50", lambda r: round(r["gp50"] - r["bp50"], 1)),
                                  ("upstream p99", lambda r: r["bp99"]), ("gateway p99", lambda r: r["gp99"]),
                                  ("added p99", lambda r: round(r["gp99"] - r["bp99"], 1))]))

    if "burst" in doc:
        b = doc["burst"]
        parts.append(f'<h2>Bursts</h2><p>{esc(b["base_rps"])} requests/s with three 10-second bursts to '
                     f'{esc(b["peak_rps"])} requests/s.</p>')
        parts.append(table([b["k6"]], [("requests", lambda r: r["requests"]), ("non-2xx", lambda r: r["non2xx"]),
                                       ("dropped", lambda r: r["dropped"]), ("p50 ms", lambda r: r["p50_ms"]),
                                       ("p95 ms", lambda r: r["p95_ms"]), ("p99 ms", lambda r: r["p99_ms"]),
                                       ("max ms", lambda r: r["max_ms"])]))

    for kind, title in (("consumer-kill", "Metering consumer killed mid-run"), ("broker-restart", "Kafka broker restarted mid-run")):
        if kind not in doc:
            continue
        c = doc[kind]
        rec = c["reconciliation"]
        parts.append(f"<h2>{esc(title)}</h2><p>{esc(c['rate'])} requests/s for {esc(c['duration_s'])} s. Fault: "
                     f"<code>{esc(c['fault'])}</code>.</p>")
        parts.append(table([c], [
            ("successful responses (k6)", lambda r: r["reconciliation"]["k6_ok_responses"]),
            ("billed events (Postgres)", lambda r: r["reconciliation"]["billed_events"]),
            ("lost", lambda r: r["reconciliation"]["lost"]),
            ("double-billed", lambda r: r["reconciliation"]["double_billed"]),
            ("peak consumer lag", lambda r: r["peak_consumer_lag"]),
            ("lag back under 100 after recovery (s)", lambda r: r["lag_settled_after_service_back_s"]),
            ("gateway publish failures", lambda r: int(r["gateway_events_failed"])),
            ("non-2xx (k6)", lambda r: r["k6"]["non2xx"]),
            ("p99 ms", lambda r: r["k6"]["p99_ms"]),
        ]))
        parts.append(f'<p class="small">Rollups match events: {esc(rec["rollup_requests_match"])}. Monthly totals match: '
                     f'{esc(rec["month_requests_match"])}. Tokens match across usage_event, tenant_month_usage and Redis: '
                     f'{esc(rec["tokens_match"])}.</p>')

    history = [("send on the request thread", doc.get("broker-restart-sync-publish"))]
    before_retry = os.path.join(HERE, "raw", "before-retry", "broker-restart.json")
    if os.path.exists(before_retry):
        with open(before_retry) as f:
            history.append(("dispatcher, no metadata retry", json.load(f)))
    history.append(("dispatcher with retry (final)", doc.get("broker-restart")))
    history = [(n, v) for n, v in history if v]
    if len(history) > 1:
        parts.append("<h2>Broker restart: how the publish path got here</h2><p>The same fault before each change in "
                     "<code>docs/DESIGN.md</code>, step 6. Host load is the 1-minute load average when the run was recorded.</p>")
        parts.append(table([{"name": n, **v} for n, v in history], [
            ("publish path", lambda r: r["name"]), ("lost", lambda r: r["reconciliation"]["lost"]),
            ("non-2xx", lambda r: r["k6"]["non2xx"]), ("dropped", lambda r: r["k6"]["dropped"]),
            ("p99 ms", lambda r: r["k6"]["p99_ms"]), ("max ms", lambda r: r["k6"]["max_ms"]),
            ("host load", lambda r: r["hardware"].get("host_load_avg_1m_at_record", "")),
        ]))

    caps = [(k, doc[k]) for k in ("capacity-1cpu", "capacity-2cpu", "capacity-4cpu-inflight256", "capacity-4cpu-inflight512",
                                  "capacity-4cpu") if k in doc]
    if caps:
        parts.append("<h2>Capacity by CPU count</h2><p>Each rate held for 45 s while every container was sampled. "
                     "CPU per request is the container's mean cores x 1000 / requests served. With 4 CPUs and the "
                     "in-flight limit fixed at 256, throughput stopped at about 256 / mean latency (Little's law) with "
                     "CPU to spare; the default is now 128 permits per CPU.</p>")
        rows = []
        for k, v in caps:
            for st in v["steps"]:
                u, ms = st["usage"], st["cpu_ms_per_request"]
                rows.append({"cpus": v["gateway_cpus"], "lim": v.get("max_in_flight", ""), "rps": st["target_rps"],
                             "ok": st["ok_rps"], "shed": st["non2xx"], "drop": st["dropped"], "p99": st["ok_p99_ms"],
                             "gw": u["gateway"]["cpu_cores"], "gwms": ms["gateway"], "rms": ms["redis"],
                             "mem": u["gateway"]["mem_mib_max"]})
        parts.append(table(rows, [("gateway CPUs", lambda r: r["cpus"]), ("in-flight limit", lambda r: r["lim"]),
                                  ("offered rps", lambda r: r["rps"]), ("served rps", lambda r: r["ok"]),
                                  ("shed (503)", lambda r: r["shed"]), ("dropped", lambda r: r["drop"]),
                                  ("p99 of 2xx ms", lambda r: r["p99"]), ("gateway cores", lambda r: r["gw"]),
                                  ("gateway CPU ms/req", lambda r: r["gwms"]), ("Redis CPU ms/req", lambda r: r["rms"]),
                                  ("gateway MiB", lambda r: r["mem"])]))

    if "drain" in doc:
        dr = doc["drain"]
        rec = dr["reconciliation"]
        parts.append(f'<h2>Backlog drain</h2><p>Metering stopped while k6 sent {esc(dr["k6"]["rps"])} requests/s, then started '
                     f'against a backlog of {esc(dr["backlog_events"])} events, with metering limited to '
                     f'{esc(dr["metering_cpu_limit"])} CPU. Rate is {esc(dr["drain_rate_window"])}.</p>')
        parts.append(table([dr], [("backlog", lambda r: r["backlog_events"]), ("first row after start s", lambda r: r["first_row_after_start_s"]),
                                  ("events/s", lambda r: r["drain_events_per_s"]),
                                  ("metering cores", lambda r: r["usage_while_draining"]["metering"]["cpu_cores"]),
                                  ("Postgres cores", lambda r: r["usage_while_draining"]["postgres"]["cpu_cores"]),
                                  ("billed", lambda r: rec["billed_events"]), ("lost", lambda r: rec["lost"]),
                                  ("double-billed", lambda r: rec["double_billed"])]))

    streams_recs = [(k, doc[k]) for k in sorted(doc) if k == "streams" or k.startswith("streams-")]
    if streams_recs:
        parts.append("<h2>Concurrent streams</h2><p>Many SSE completions open at once, each streamed by the mock over "
                     "30 s in 30 chunks, started evenly over the ramp and read by <code>loadtest/Streams.java</code> "
                     "(virtual threads). A run stops at the first step with more than 1% failures.</p>")
        rows = []
        for k, v in streams_recs:
            for st in v["steps"]:
                rows.append({"mem": v.get("gateway_mem_limit", "1g"), "n": st["streams"], "ok": st["completed"],
                             "fail": st["failed"], "peak": st["peak_open"],
                             "fe": (st.get("first_event_ms") or {}).get("p99", ""),
                             "gm": st["gateway"]["mem_mib_max"], "lost": st["reconciliation"]["lost"],
                             "dbl": st["reconciliation"]["double_billed"]})
        parts.append(table(rows, [("gateway memory", lambda r: r["mem"]), ("streams", lambda r: r["n"]),
                                  ("completed", lambda r: r["ok"]), ("failed", lambda r: r["fail"]),
                                  ("peak open", lambda r: r["peak"]), ("first event p99 ms", lambda r: r["fe"]),
                                  ("gateway MiB max", lambda r: r["gm"]), ("lost", lambda r: r["lost"]),
                                  ("double-billed", lambda r: r["dbl"])]))

    races = [(k, doc[k]) for k in ("quota-race-v1", "quota-race-v2") if k in doc]
    if races:
        parts.append("<h2>Quota race: one tenant, a fixed token quota, hit hard</h2><p>A tenant with a 300,000-token "
                     "monthly quota receives requests for 10 s at each rate; every figure is read from Postgres after the "
                     "consumer lag reaches zero. v1 admitted against the last committed total, so requests in flight and "
                     "events still in Kafka were invisible to it. v2 reserves each request's worst case at admission "
                     "(prompt plus granted output), caps the output sent upstream at what the quota can still pay for, "
                     "and settles to the real usage. Both versions ran on battery power on the same machine the same morning.</p>")
        rows = [dict(r, ver=k.rsplit("-", 1)[1]) for k, v in races for r in v["runs"]]
        parts.append(table(rows, [("version", lambda r: r["ver"]), ("rate", lambda r: r["rate"]),
                                  ("quota", lambda r: r["quota_tokens"]), ("billed", lambda r: r["billed_tokens"]),
                                  ("past quota", lambda r: r["overshoot_tokens"]), ("past quota %", lambda r: r["overshoot_pct"]),
                                  ("requests served", lambda r: r["billed_ok_requests"])]))

    kills = [(k, doc[k]) for k in sorted(doc) if k.startswith("gateway-kill-") and k != "gateway-kill-warmup"]
    if kills:
        parts.append("<h2>Gateway SIGKILL: every request the provider served, accounted for</h2><p>The gateway is killed "
                     "with SIGKILL mid-run and restarted. The mock provider records the request id of every request it "
                     "served; once the killed requests' leases expire and are reaped, each one is looked up in Postgres. "
                     "A version that sends no request id is reconciled by counts.</p>")
        parts.append(table([dict(v, key=k) for k, v in kills], [
            ("version", lambda r: r["version"]), ("rate", lambda r: r["rate"]),
            ("provider served", lambda r: r["provider_served_requests"]),
            ("billed with tokens", lambda r: r["billed_rows_with_tokens"]),
            ("served, not billed", lambda r: r.get("served_but_not_billed", r["unbilled_served_requests_by_count"])),
            ("billed at reservation", lambda r: r.get("reaped_leases_billed_at_reservation", "")),
            ("over-billed tokens", lambda r: r.get("reaped_overbilled_tokens", "")),
            ("exact provider rows", lambda r: (f'{r["provider_rows_matching_exactly"]} / {r["provider_rows"]}'
                                               if "provider_rows" in r else "")),
            ("all billed at s", lambda r: r["all_billed_at_s"]),
        ]))

    cost = [(k, doc[k]) for k in ("capacity-2cpu-v2-ac-a", "capacity-2cpu-v1-ac", "capacity-2cpu-v2-ac-b",
                                  "capacity-2cpu-v1-battery-a", "capacity-2cpu-v2-battery") if k in doc]
    if cost:
        parts.append("<h2>What leases cost</h2><p>The same 2-CPU gateway before leases (v1) and with them (v2), run back to back "
                     "on the same machine, each after a 30 s warm-up. CPU-ms per request is the container's average cores times "
                     "1000 over the requests it served. The battery pair is noisier; its v2 1000 req/s step followed a slow warm-up "
                     "and is left out.</p>")
        rows = []
        for k, v in cost:
            for st in v["steps"]:
                if k == "capacity-2cpu-v2-battery" and st["target_rps"] == 1000:
                    continue
                c, u = st.get("cpu_ms_per_request", {}), st.get("usage", {})
                rows.append(dict(run=k.replace("capacity-2cpu-", ""), rate=st["target_rps"], served=st["ok_rps"],
                                 shed=st.get("non2xx", ""), p99=st.get("ok_p99_ms", ""), gw=c.get("gateway"), redis=c.get("redis"),
                                 mem=u.get("gateway", {}).get("mem_mib_max")))
        parts.append(table(rows, [("run", lambda r: r["run"]), ("offered", lambda r: r["rate"]), ("served/s", lambda r: r["served"]),
                                  ("shed", lambda r: r["shed"]), ("admitted p99 ms", lambda r: r["p99"]),
                                  ("gateway CPU-ms", lambda r: r["gw"]), ("Redis CPU-ms", lambda r: r["redis"]),
                                  ("gateway MiB", lambda r: r["mem"])]))
    if "coldstart-v1-vs-v2" in doc:
        cs = doc["coldstart-v1-vs-v2"]
        parts.append("<h3>Right after a restart</h3><p>Each gateway recreated 5 s before 30 s at 1,000 req/s, then 30 s more, "
                     "in the order v1, v2, v2, v1. Shed requests got an immediate 503 from the in-flight limit while the JIT "
                     "compiled the hot path.</p>")
        parts.append(table(cs["runs"], [("order", lambda r: r["order"]), ("version", lambda r: r["version"]),
                                        ("window", lambda r: r["window"]), ("served", lambda r: r["ok"]),
                                        ("shed", lambda r: r["shed"]), ("admitted p50 ms", lambda r: r["ok_p50_ms"]),
                                        ("admitted p99 ms", lambda r: r["ok_p99_ms"])]))

    reals = [(k, doc[k]) for k in sorted(doc) if k.startswith("real-model-")]
    for k, v in reals:
        d, q = v["drift"], v["quota_race"]
        ratio = "learned_prompt_excess_tokens" not in d
        title = f'A real model: {esc(v["model"])} on Ollama' + (" (first calibration, a ratio, since replaced)" if ratio else "")
        parts.append(f'<h2>{title}</h2><p>{esc(d["requests"])} requests (10 prompt shapes: '
                     f'system messages, multi-turn, code, JSON; half streamed) through the gateway to a local model, with '
                     f'every output also counted by the gateway. Error is the gateway\'s count minus the model\'s reported '
                     f'count, as a percentage of the model\'s. The gateway counts with OpenAI\'s o200k vocabulary, so for a '
                     f'non-OpenAI model the prompt error includes the model\'s own chat template.</p>')
        pe, oe = d["prompt_estimate_error_pct"], d["output_estimate_error_pct"]
        parts.append(table([dict(pe, part="prompt"), dict(oe, part="output")], [
            ("part", lambda r: r["part"]), ("min %", lambda r: r["min"]), ("p10 %", lambda r: r["p10"]),
            ("p50 %", lambda r: r["p50"]), ("p90 %", lambda r: r["p90"]), ("max %", lambda r: r["max"])]))
        learned = (f'Prompt ratio the first calibration learned (provider count over the gateway\'s): {esc(d.get("learned_prompt_ratio"))}, '
                   f'so every prompt was reserved at about three times its size.' if ratio else
                   f'Prompt tokens the gateway learned to add for this model by the end of the run: {esc(round(d["learned_prompt_excess_tokens"], 1))}.')
        parts.append(f'<p class="small">{learned} Usage sources: {esc(d["usage_sources"])}.</p>')
        parts.append(table([q], [("quota", lambda r: r["quota_tokens"]), ("requests at once", lambda r: r["requests"]),
                                 ("served", lambda r: r["served"]), ("refused (quota)", lambda r: r["refused_quota"]),
                                 ("served with reduced max_tokens", lambda r: r["served_with_reduced_max_tokens"]),
                                 ("quota_reserved retries", lambda r: r.get("retries_while_reserved", "not counted")),
                                 ("billed", lambda r: r["billed_tokens"]), ("past quota", lambda r: r["overshoot_tokens"])]))

    k8s_runs = []
    for label, rel in (("cold start", ("k8s", "cold-start", "summary.json")), ("warm", ("k8s", "summary.json"))):
        path = os.path.join(HERE, *rel)
        if os.path.exists(path):
            with open(path) as f:
                k8s_runs.append(dict(json.load(f), run=label))
    if k8s_runs:
        k = k8s_runs[-1]
        parts.append(f'<h2>On Kubernetes (k3s)</h2><p>The same images on k3s ({esc("; ".join(k["nodes"]))}), '
                     f'the gateway behind a Service with a CPU autoscaler (2 to 4 replicas), k6 running as a Job inside the '
                     f'cluster, {esc(k["rate"])} requests/s for {esc(k["duration"])}. The cold-start run began 20 s after the pods '
                     f'did: JIT compilation pushed CPU over the autoscaler target, it scaled the gateway to 4 replicas 39 s in, '
                     f'and p99 carried the warm-up. The warm run repeated it on the same pods. Both ran on battery power, '
                     f'so treat the latencies as indicative.</p>')
        parts.append(table(k8s_runs, [("run", lambda r: r["run"]), ("requests", lambda r: r["k6"]["requests"]),
                                      ("non-2xx", lambda r: r["k6"]["non2xx"]), ("p50 ms", lambda r: r["k6"]["p50_ms"]),
                                      ("p99 ms", lambda r: r["k6"]["p99_ms"]), ("gateway replicas", lambda r: r["gateway_replicas"]),
                                      ("billed", lambda r: r["reconciliation"]["billed_events"]),
                                      ("lost", lambda r: r["reconciliation"]["lost"]),
                                      ("double-billed", lambda r: r["reconciliation"]["double_billed"])]))

    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Gateway Load Results</title>
<style>
:root{{--bg:#fbfaf7;--fg:#1d1f23;--muted:#5b616b;--line:#e2e0da;--card:#fff;--a:#1f5fbf;--b:#b3261e;--c:#1d7a46}}
@media (prefers-color-scheme: dark){{:root:not([data-theme="light"]){{--bg:#15171a;--fg:#e7e9ec;--muted:#a2a9b3;--line:#33373d;--card:#1d2024;--a:#7fb0ff;--b:#ff8a80;--c:#6fd39b}}}}
:root[data-theme="dark"]{{--bg:#15171a;--fg:#e7e9ec;--muted:#a2a9b3;--line:#33373d;--card:#1d2024;--a:#7fb0ff;--b:#ff8a80;--c:#6fd39b}}
body{{margin:0;background:var(--bg);color:var(--fg);font:16px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}}
main{{max-width:900px;margin:0 auto;padding:24px 16px 56px}} h1{{margin:0 0 6px}} h2{{margin-top:36px}}
.sub,.small{{color:var(--muted)}} .small{{font-size:.88rem}}
.card{{background:var(--card);border:1px solid var(--line);border-left:4px solid var(--a);border-radius:8px;padding:12px 16px}}
.tw{{overflow-x:auto;border:1px solid var(--line);border-radius:8px;background:var(--card)}}
table{{border-collapse:collapse;width:100%;font-size:.88rem}} th,td{{padding:6px 10px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}}
th{{font-weight:600}} code{{font-size:.85em}}
svg{{width:100%;height:auto;background:var(--card);border:1px solid var(--line);border-radius:8px}}
svg text{{font-size:11px;fill:var(--muted)}} .grid{{stroke:var(--line)}}
polyline{{stroke-width:2;fill:none}} polyline.p50{{stroke:var(--c)}} polyline.p99{{stroke:var(--b)}} polyline.base{{stroke:var(--a);stroke-dasharray:4 3}}
circle.p50{{fill:var(--c)}} circle.p99{{fill:var(--b)}} circle.base{{fill:var(--a)}}
.legend .k{{display:inline-block;width:14px;height:3px;margin:0 6px 3px 12px;vertical-align:middle}}
.k.p50{{background:var(--c)}} .k.p99{{background:var(--b)}} .k.base{{background:var(--a)}}
</style></head><body><main>
<h1>Gateway Load Results</h1>
{''.join(parts)}
<p class="small">Generated by <code>results/build_page.py</code> from <code>results/summary.json</code>; raw k6 summaries are produced in
<code>results/raw/</code> by <code>loadtest/bench.py</code>.</p>
</main></body></html>
"""
    with open(os.path.join(HERE, "index.html"), "w") as f:
        f.write(page)
    print("wrote results/index.html")


if __name__ == "__main__":
    main()
