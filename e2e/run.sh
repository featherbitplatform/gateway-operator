#!/usr/bin/env bash
# kind-based end-to-end: a file-mode and an etcd-mode gateway driven by the operator.
#
# Needs docker, kind, helm and kubectl. Env overrides:
#   CLUSTER                kind cluster name
#   GATEWAY_CHART          gateway chart; must have config.gatewayConfigMap (gateway develop,
#                          released after 0.15.0). Local path or CI checkout of featherbitplatform/gateway.
#   GATEWAY_CHART_VERSION  only for an OCI GATEWAY_CHART (oci://ghcr.io/featherbitplatform/charts/featherbit-gateway)
#                          once a release contains config.gatewayConfigMap; ignored for a path.
#   GATEWAY_IMAGE_TAG      gateway image tag (edge is published on gateway develop pushes)
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=${CLUSTER:-fb-operator-e2e}
# Default: sibling checkout of the gateway repo on develop (override with the path of a develop checkout).
GATEWAY_CHART=${GATEWAY_CHART:-../gateway/charts/featherbit-gateway}
GATEWAY_IMAGE_TAG=${GATEWAY_IMAGE_TAG:-edge}
IMAGE=featherbit-operator:e2e
CHART_VERSION_ARGS=()
if [ -n "${GATEWAY_CHART_VERSION:-}" ]; then CHART_VERSION_ARGS=(--version "$GATEWAY_CHART_VERSION"); fi
PF_PIDS=()

fail() {
  echo "FAIL: $1" >&2
  kubectl -n gw get featherbitgateways -o yaml || true
  kubectl -n gw get pods || true
  kubectl -n gw logs deploy/op-featherbit-operator --tail=100 || true
  exit 1
}
wait_cond() { kubectl -n "$1" wait --for=condition="$3" "$2" --timeout=120s || fail "$2 never reached $3"; }
cleanup() {
  for p in "${PF_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  if [ "${KEEP_CLUSTER:-}" != 1 ]; then kind delete cluster --name "$CLUSTER" || true; fi
}
# Polls GET $2 on local port $1 until the body contains "operator".
wait_served() {
  local port=$1 path=$2 tries=$3
  for i in $(seq 1 "$tries"); do
    if curl -fsS "http://localhost:$port$path" 2>/dev/null | grep -q operator; then return 0; fi
    sleep 2
  done
  return 1
}

kind create cluster --name "$CLUSTER" --wait 120s
trap cleanup EXIT
docker build -t "$IMAGE" .
kind load docker-image "$IMAGE" --name "$CLUSTER"

kubectl create ns gw
kubectl create ns shop
helm install op charts/featherbit-operator -n gw \
  --set image.repository=featherbit-operator --set image.tag=e2e --wait --timeout 180s
# The operator's Deployment name is derived from the release; fail() logs it.

# 1. Happy path (ConfigMap sink). The ConfigMap is optional at pod start, so
#    the gateway comes up before the operator has rendered anything.
helm install edge "$GATEWAY_CHART" ${CHART_VERSION_ARGS[@]+"${CHART_VERSION_ARGS[@]}"} -n gw \
  --set image.tag="$GATEWAY_IMAGE_TAG" \
  --set config.gatewayConfigMap=edge-gateway-config --set tests.dataPlanePath="" \
  --wait --timeout 180s

kubectl apply -f e2e/samples/gateway-configmap.yaml -f e2e/samples/policy-hello.yaml -f e2e/samples/route-hello.yaml
wait_cond gw featherbitgateway/edge Ready
wait_cond shop route/hello Programmed
kubectl -n gw get configmap edge-gateway-config -o jsonpath='{.data.gateway\.yaml}' | grep -q 'via": "operator' \
  || fail "ConfigMap not rendered"
kubectl -n gw port-forward svc/edge-featherbit-gateway 18080:80 >/dev/null 2>&1 &
PF_PIDS+=($!)
sleep 2
# kubelet ConfigMap refresh + gateway hot-reload take up to ~1 min
wait_served 18080 /hello 60 || fail "data plane never served the operator-rendered route"

# 2. Webhook rejection and partial render.
if kubectl apply -f e2e/samples/policy-bad.yaml 2>err.txt; then fail "bad policy was admitted"; fi
grep -q preflight err.txt || fail "rejection message lacks the port name: $(cat err.txt)"
kubectl apply -f e2e/samples/route-orphan.yaml
kubectl -n shop wait \
  --for=jsonpath='{.status.conditions[?(@.type=="ResolvedRefs")].reason}'=PolicyNotFound \
  route/orphan --timeout=60s || fail "orphan route not flagged"
wait_cond shop route/hello Programmed
wait_cond gw featherbitgateway/edge Ready

# 3. etcd mode, two replicas. gatewayRaw seeds an empty prefix with valid YAML
#    (config.gateway=null would render the text "null").
kubectl apply -f e2e/samples/etcd.yaml
kubectl -n gw rollout status deploy/etcd --timeout=120s
helm install edge-etcd "$GATEWAY_CHART" ${CHART_VERSION_ARGS[@]+"${CHART_VERSION_ARGS[@]}"} -n gw \
  --set image.tag="$GATEWAY_IMAGE_TAG" \
  --set config.source=etcd --set 'config.etcd.endpoints[0]=http://etcd.gw.svc:2379' --set config.etcd.prefix=/e2e \
  --set-string config.gatewayRaw='routes: []' --set replicaCount=2 --set tests.dataPlanePath="" \
  --wait --timeout 180s
kubectl apply -f e2e/samples/gateway-etcd.yaml
wait_cond gw featherbitgateway/edge-etcd Ready
kubectl -n gw port-forward svc/edge-etcd-featherbit-gateway 18081:80 >/dev/null 2>&1 &
PF_PIDS+=($!)
sleep 2
wait_served 18081 /hello 15 || fail "etcd-mode gateway never served the route"
echo "e2e: OK"
