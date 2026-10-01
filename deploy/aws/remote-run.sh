#!/usr/bin/env bash
# Runs on the k3s node through SSM: deploys the images CI pushed, runs the in-cluster bench and
# uploads the results. Usage: remote-run.sh <registry> <tag> <bucket> <run-id> <rate> <duration>
set -euo pipefail
REGISTRY=$1 TAG=$2 BUCKET=$3 RUN=$4 RATE=${5:-1000} DURATION=${6:-120s}
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml PATH=/usr/local/bin:$PATH
until [ -f /var/run/llmgw-k3s-ready ]; do sleep 5; done

WORK=$(mktemp -d)
aws s3 cp "s3://$BUCKET/runs/$RUN/bundle.tgz" "$WORK/bundle.tgz" --only-show-errors
tar -xzf "$WORK/bundle.tgz" -C "$WORK"
cd "$WORK"

mkdir -p deploy/k8s/overlays/aws
cat > deploy/k8s/overlays/aws/kustomization.yaml <<YAML
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: [../../]
images:
  - { name: llmgw/gateway, newName: $REGISTRY/llmgw/gateway, newTag: "$TAG" }
  - { name: llmgw/metering, newName: $REGISTRY/llmgw/metering, newTag: "$TAG" }
  - { name: llmgw/mock-upstream, newName: $REGISTRY/llmgw/mock-upstream, newTag: "$TAG" }
YAML
kubectl apply -k deploy/k8s/overlays/aws
for d in postgres redis mock-upstream gateway metering; do
  kubectl -n llmgw rollout status deploy/$d --timeout=600s
done
kubectl -n llmgw rollout status statefulset/kafka --timeout=600s

STATUS=0
RATE=$RATE DURATION=$DURATION OUT=results/aws deploy/k8s/bench-on-cluster.sh || STATUS=$?
uname -m > results/aws/arch.txt
TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-type > results/aws/instance-type.txt
aws s3 cp results/aws "s3://$BUCKET/runs/$RUN/results" --recursive --only-show-errors
exit $STATUS
