# featherbit-operator

Helm chart for the featherbit gateway operator: installs the `featherbit.io/v1alpha1` CRDs
(from `crds/`), the operator Deployment, RBAC and the validating admission webhook.

```bash
helm install featherbit-operator oci://ghcr.io/featherbitplatform/charts/featherbit-operator
```

## Webhook TLS

By default Helm generates a CA and serving certificate (Secret `<release>-webhook-tls`, kept
across upgrades via `lookup` and `helm.sh/resource-policy: keep`) and sets the webhook
`caBundle` from the same CA. Under ArgoCD/Flux, where `lookup` is unavailable, enable
cert-manager instead:

```bash
helm install featherbit-operator ... --set webhook.certManager.enabled=true \
  --set webhook.certManager.issuerRef.name=<issuer>
```

The Deployment uses `strategy: Recreate` (single replica, no leader election), so a rollout has a
short window where `featherbit.io` creates/updates are rejected by the fail-closed webhook.

`helm uninstall` keeps the webhook TLS Secret (`helm.sh/resource-policy: keep`). A reinstall
under the same release name and namespace adopts it (Helm's ownership labels and annotations
match) and `lookup` reuses the existing certificate pair. Only a different release name or
`fullnameOverride` yields a different Secret name, generates a new pair and leaves the old
Secret behind to delete manually.

## Gateways the operator feeds

This chart installs the operator only; gateways stay on the `featherbit-gateway` chart.

- File mode: install the gateway chart with `config.gatewayConfigMap=<name>` and give the `FeatherbitGateway` `spec.sink.configMap.name: <name>`.
- etcd mode: install the gateway chart with `config.source=etcd`, `config.etcd.endpoints` and `config.gateway: {}` (an empty seed, so a pod booting against an empty prefix seeds nothing; a non-empty seed is replaced by the first reconcile), and give the `FeatherbitGateway` `spec.sink.etcd` the same endpoints and prefix.

Full walkthrough: https://featherbitplatform.github.io/gateway/operator/getting-started

## Values

| Key | Default | Description |
|---|---|---|
| `image.repository` / `image.tag` / `image.digest` | `featherbit/operator` / appVersion / `""` | Operator image. |
| `replicaCount` | `1` | Fixed at 1 until leader election exists (schema enforces it). |
| `logging.format` / `logging.level` | `text` / `info` | `text` or `json`; `RUST_LOG` level. |
| `webhook.failurePolicy` | `Fail` | `Fail` or `Ignore`. |
| `webhook.timeoutSeconds` | `10` | Admission timeout. |
| `webhook.certManager.enabled` | `false` | Use cert-manager instead of Helm-generated certs. |
| `webhook.certManager.issuerRef` | `{name: selfsigned, kind: Issuer}` | Issuer for the webhook certificate. |
| `metrics.serviceMonitor.enabled` | `false` | Create a Prometheus Operator ServiceMonitor. |
| `rbac.create` / `serviceAccount.*` | `true` | RBAC and ServiceAccount. |
| `resources`, `nodeSelector`, `tolerations`, `affinity` | see `values.yaml` | Scheduling. |
| `tests.enabled` | `true` | `helm test` pod probing `/readyz`. |
