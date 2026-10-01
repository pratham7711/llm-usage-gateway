#!/usr/bin/env bash
# Creates a local k3s cluster (k3d), loads the locally built images and deploys the stack.
set -euo pipefail
cd "$(dirname "$0")/../.."
CLUSTER=llmgw
if ! k3d cluster list "$CLUSTER" >/dev/null 2>&1; then
  k3d cluster create "$CLUSTER" --agents 1 -p "8088:80@loadbalancer" --wait
fi
# Save one platform only: with Docker's containerd image store, a plain save writes the full
# multi-platform index, k3s's ctr fails on the layers that were never pulled ("content digest
# not found"), and k3d still reports success.
TAR="$(mktemp -d)/llmgw-images.tar"
docker save --platform "linux/$(docker version --format '{{.Server.Arch}}')" -o "$TAR" \
  llmgw/gateway:dev llmgw/metering:dev llmgw/mock-upstream:dev \
  apache/kafka:3.9.1 postgres:17-alpine redis:7.4-alpine grafana/k6:2.3.0
k3d image import -c "$CLUSTER" "$TAR"
rm -rf "$(dirname "$TAR")"
kubectl apply -k deploy/k8s
for d in postgres redis mock-upstream gateway metering; do
  kubectl -n llmgw rollout status deploy/$d --timeout=300s
done
kubectl -n llmgw rollout status statefulset/kafka --timeout=300s
echo "gateway: http://localhost:8088/v1/chat/completions"
