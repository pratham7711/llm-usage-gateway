#!/usr/bin/env bash
# Runs one k6 constant-arrival-rate step against the gateway from inside the cluster, then
# reconciles what k6 saw with what metering billed. Works on any cluster the manifests run on:
# k3d locally, or the k3s instance in deploy/terraform.
#
#   RATE=500 DURATION=60s OUT=results/k8s deploy/k8s/bench-on-cluster.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
NS=llmgw
RATE=${RATE:-500}
DURATION=${DURATION:-60s}
OUT=${OUT:-results/k8s}
mkdir -p "$OUT"

psql() { kubectl -n $NS exec deploy/postgres -- psql -U usage -d usage -At -c "$1"; }
lag() {
  kubectl -n $NS exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group metering 2>/dev/null | awk '$1=="metering" && $2=="usage-events" {s+=$6} END {print s+0}'
}

psql "truncate usage_event, usage_rollup_minute, tenant_month_usage" >/dev/null
kubectl -n $NS create configmap k6-scripts --from-file=loadtest/common.js --from-file=loadtest/step.js \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n $NS delete job k6 --ignore-not-found >/dev/null

kubectl -n $NS apply -f - >/dev/null <<YAML
apiVersion: batch/v1
kind: Job
metadata: { name: k6 }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: k6
          image: grafana/k6:2.3.0
          args: ["run", "--quiet", "/scripts/step.js"]
          env:
            - { name: GATEWAY_URL, value: "http://gateway.$NS.svc" }
            - { name: RATE, value: "$RATE" }
            - { name: DURATION, value: "$DURATION" }
            - { name: VUS, value: "500" }
            - { name: NAME, value: "k8s-step-$RATE" }
            - { name: SUMMARY_STDOUT, value: json }
          volumeMounts:
            - { name: scripts, mountPath: /scripts }
            - { name: results, mountPath: /results }
      volumes:
        - { name: scripts, configMap: { name: k6-scripts } }
        - { name: results, emptyDir: {} }
YAML

echo "k6: $RATE rps for $DURATION in-cluster"
kubectl -n $NS wait --for=condition=complete job/k6 --timeout=900s >/dev/null
kubectl -n $NS logs job/k6 > "$OUT/k6.log"
grep '^k8s-step' "$OUT/k6.log"
grep '^K6_SUMMARY_JSON ' "$OUT/k6.log" | sed 's/^K6_SUMMARY_JSON //' > "$OUT/k6-summary.json"

for _ in $(seq 1 120); do [ "$(lag)" = "0" ] && break; sleep 2; done
sleep 3

OK=$(python3 -c "import json,sys; print(int(json.load(open(sys.argv[1]))['metrics']['ok_responses']['values']['count']))" "$OUT/k6-summary.json")
EVENTS=$(psql "select count(*) from usage_event")
DISTINCT=$(psql "select count(distinct event_id) from usage_event")
ROLLUP=$(psql "select coalesce(sum(requests),0) from usage_rollup_minute")
MONTH=$(psql "select coalesce(sum(requests),0) from tenant_month_usage")
NODE=$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name} {.status.nodeInfo.kubeletVersion} {.status.capacity.cpu}cpu {.status.capacity.memory}{"\n"}{end}')
GW_PODS=$(kubectl -n $NS get deploy gateway -o jsonpath='{.status.readyReplicas}')

python3 - "$OUT" "$RATE" "$DURATION" "$OK" "$EVENTS" "$DISTINCT" "$ROLLUP" "$MONTH" "$NODE" "$GW_PODS" <<'PY'
import json, sys
out, rate, duration, ok, events, distinct, rollup, month, node, pods = sys.argv[1:]
m = json.load(open(f"{out}/k6-summary.json"))["metrics"]
d = m["http_req_duration"]["values"]
okd = m.get("http_req_duration{expected_response:true}", {}).get("values", {})
ok, events, distinct, rollup, month = map(int, (ok, events, distinct, rollup, month))
res = {
    "rate": int(rate), "duration": duration, "gateway_replicas": int(pods or 0), "nodes": node.strip().splitlines(),
    "k6": {"requests": m["http_reqs"]["values"]["count"], "ok": ok,
           "non2xx": int(m.get("non2xx_responses", {}).get("values", {}).get("count", 0)),
           "dropped": int(m.get("dropped_iterations", {}).get("values", {}).get("count", 0)),
           "p50_ms": round(d["med"], 1), "p99_ms": round(d["p(99)"], 1),
           "ok_p99_ms": round(okd["p(99)"], 1) if okd else None},
    "reconciliation": {"billed_events": events, "lost": ok - events, "double_billed": events - distinct,
                       "rollup_requests_match": rollup == events, "month_requests_match": month == events},
}
json.dump(res, open(f"{out}/summary.json", "w"), indent=2)
print(json.dumps(res["reconciliation"]))
bad = res["reconciliation"]["lost"] or res["reconciliation"]["double_billed"] or not (
    res["reconciliation"]["rollup_requests_match"] and res["reconciliation"]["month_requests_match"])
sys.exit(1 if bad else 0)
PY
