#!/usr/bin/env bash
# dev/chart-render-check.sh — assertions on `helm template` output.
set -euo pipefail
CHART=charts/featherbit-operator
fail() { echo "FAIL: $1" >&2; exit 1; }

out=$(helm template op "$CHART")
grep -q 'kind: ValidatingWebhookConfiguration' <<<"$out" || fail "no webhook configuration"
grep -q 'failurePolicy: Fail' <<<"$out" || fail "webhook must fail closed by default"
grep -q 'caBundle: ' <<<"$out" || fail "self-signed CA bundle not injected"
grep -q 'path: /validate/featherbit.io/v1alpha1/Policy' <<<"$out" || fail "Policy webhook path missing"
for k in Route Policy Supernode PluginConfig Store Consumer FeatherbitGateway; do
  grep -q "/validate/featherbit.io/v1alpha1/$k" <<<"$out" || fail "webhook for $k missing"
done
grep -q 'readOnlyRootFilesystem: true' <<<"$out" || fail "security context missing"
grep -q 'runAsUser: 65532' <<<"$out" || fail "nonroot uid missing"
grep -q -e '--webhook-addr' <<<"$out" && grep -q '0.0.0.0:9443' <<<"$out" || fail "webhook addr arg missing"
grep -q 'containerPort: 9443' <<<"$out" || fail "webhook port missing"
grep -q 'path: /readyz' <<<"$out" || fail "readiness probe missing"
grep -q 'kind: ClusterRole' <<<"$out" || fail "ClusterRole missing"
grep -A3 'resources:' <<<"$out" | grep -q 'featherbitgateways/status' || fail "status subresource RBAC missing"
grep -q 'kind: Secret' <<<"$out" || fail "webhook cert Secret missing"
grep -q 'kind: Certificate' <<<"$out" && fail "cert-manager objects must be off by default"

# The CA published to the API server must be the one that signed the served pair.
secret_ca=$(sed -n 's/^  ca\.crt: //p' <<<"$out" | head -n1)
[ -n "$secret_ca" ] || fail "Secret ca.crt missing"
bundles=$(sed -n 's/^ *caBundle: //p' <<<"$out" | sort -u)
[ "$bundles" = "$secret_ca" ] || fail "webhook caBundle differs from Secret ca.crt (CA computed twice?)"

cm=$(helm template op "$CHART" --set webhook.certManager.enabled=true --api-versions cert-manager.io/v1)
grep -q 'kind: Certificate' <<<"$cm" || fail "cert-manager Certificate missing"
grep -q 'cert-manager.io/inject-ca-from' <<<"$cm" || fail "inject-ca-from annotation missing"
grep -q 'kind: Secret' <<<"$cm" && fail "Helm-generated Secret must not render with cert-manager"

helm template op "$CHART" --set replicaCount=2 >/dev/null 2>&1 && fail "replicaCount>1 must be rejected by the schema"
echo "chart render check: OK"
