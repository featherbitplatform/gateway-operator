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
