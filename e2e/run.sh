#!/usr/bin/env bash
# kind-based end-to-end: a file-mode and an etcd-mode gateway driven by the operator.
#
# Needs docker, kind, helm and kubectl. Env overrides:
#   CLUSTER                kind cluster name
#   GATEWAY_CHART          gateway chart: an OCI reference (default: the published chart) or a local
#                          path to a checkout of featherbitplatform/gateway. Needs config.gatewayConfigMap
#                          (gateway chart >= 0.16).
#   GATEWAY_CHART_VERSION  chart version for an OCI GATEWAY_CHART (default 0.16.0); ignored for a path.
#   GATEWAY_IMAGE_TAG      gateway image tag override; empty (default) uses the chart's appVersion.
#                          Use `edge` with a develop checkout of the chart.
set -euo pipefail
# Git Bash (Windows) rewrites arguments that look like POSIX paths, which turned
# `--set config.etcd.prefix=/e2e` into `C:/Program Files/Git/e2e`. No-op elsewhere.
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
CLUSTER=${CLUSTER:-fb-operator-e2e}
# Default: the published gateway chart (override with a local path to test an unreleased chart).
GATEWAY_CHART=${GATEWAY_CHART:-oci://ghcr.io/featherbitplatform/charts/featherbit-gateway}
GATEWAY_IMAGE_TAG=${GATEWAY_IMAGE_TAG:-}
IMAGE=featherbit-operator:e2e
CHART_VERSION_ARGS=()
case "$GATEWAY_CHART" in
  oci://*) CHART_VERSION_ARGS=(--version "${GATEWAY_CHART_VERSION:-0.16.0}") ;;
esac
IMAGE_TAG_ARGS=()
if [ -n "$GATEWAY_IMAGE_TAG" ]; then IMAGE_TAG_ARGS=(--set "image.tag=$GATEWAY_IMAGE_TAG"); fi
PF_PIDS=()

# Mirrored by the "Collect cluster state" step of .github/workflows/e2e.yml.
dump() {
  kubectl get routes,policies,featherbitgateways -A -o yaml || true
  kubectl -n gw get configmaps -l app.kubernetes.io/managed-by=featherbit-operator -o yaml || true
  kubectl -n gw get pods || true
  kubectl -n gw logs deploy/op-featherbit-operator --tail=200 || true
  kubectl -n gw logs -l app.kubernetes.io/name=featherbit-gateway --all-containers --tail=100 --prefix || true
}
fail() {
  echo "FAIL: $1" >&2
  dump >&2
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
# PROFILE=release skips the fat LTO of the dist profile; e2e does not ship the image.
docker build --build-arg PROFILE=release -t "$IMAGE" .
kind load docker-image "$IMAGE" --name "$CLUSTER"

kubectl create ns gw
kubectl create ns shop
helm install op charts/featherbit-operator -n gw \
  --set image.repository=featherbit-operator --set image.tag=e2e --wait --timeout 180s
# The operator's Deployment name is derived from the release; fail() logs it.

# 1. Happy path (ConfigMap sink). The ConfigMap is optional at pod start, so
#    the gateway comes up before the operator has rendered anything.
helm install edge "$GATEWAY_CHART" ${CHART_VERSION_ARGS[@]+"${CHART_VERSION_ARGS[@]}"} -n gw \
  ${IMAGE_TAG_ARGS[@]+"${IMAGE_TAG_ARGS[@]}"} \
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
# The excluded route is left out of the render and the valid route keeps serving.
if kubectl -n gw get configmap edge-gateway-config -o jsonpath='{.data.gateway\.yaml}' | grep -q orphan; then
  fail "orphan route leaked into the rendered ConfigMap"
fi
curl -fsS "http://localhost:18080/hello" | grep -q operator || fail "/hello stopped serving after the orphan route was applied"

# 3. etcd mode, two replicas. gatewayRaw seeds an empty prefix with valid YAML
#    (config.gateway=null would render the text "null").
kubectl apply -f e2e/samples/etcd.yaml
kubectl -n gw rollout status deploy/etcd --timeout=120s
helm install edge-etcd "$GATEWAY_CHART" ${CHART_VERSION_ARGS[@]+"${CHART_VERSION_ARGS[@]}"} -n gw \
  ${IMAGE_TAG_ARGS[@]+"${IMAGE_TAG_ARGS[@]}"} \
  --set config.source=etcd --set 'config.etcd.endpoints[0]=http://etcd.gw.svc:2379' --set config.etcd.prefix=/e2e \
  --set-string config.gatewayRaw='routes: []' --set replicaCount=2 --set tests.dataPlanePath="" \
  --wait --timeout 180s
kubectl apply -f e2e/samples/gateway-etcd.yaml
wait_cond gw featherbitgateway/edge-etcd Ready
kubectl -n gw wait --for=condition=Available deploy/edge-etcd-featherbit-gateway --timeout=120s \
  || fail "etcd-mode gateway never became Available"
[ "$(kubectl -n gw get deploy edge-etcd-featherbit-gateway -o jsonpath='{.status.availableReplicas}')" = 2 ] \
  || fail "etcd-mode gateway does not have 2 available replicas"
kubectl -n gw port-forward svc/edge-etcd-featherbit-gateway 18081:80 >/dev/null 2>&1 &
PF_PIDS+=($!)
sleep 2
wait_served 18081 /hello 15 || fail "etcd-mode gateway never served the route"
# Every replica must serve the route, not just whichever pod the Service picks.
port=18090
for pod in $(kubectl -n gw get pods -l app.kubernetes.io/instance=edge-etcd -o name); do
  kubectl -n gw port-forward "$pod" "$port:8080" >/dev/null 2>&1 &
  PF_PIDS+=($!)
  sleep 2
  wait_served "$port" /hello 15 || fail "$pod never served the route from etcd"
  port=$((port + 1))
done
echo "e2e: OK"
