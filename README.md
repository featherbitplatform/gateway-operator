# featherbit operator

Kubernetes operator for the [featherbit](https://github.com/featherbitplatform/gateway) API gateway. It makes the gateway's configuration a set of Kubernetes resources: routes, policies and the shared kinds (supernodes, plugin configs, stores, consumers) are namespaced custom resources in the `featherbit.io/v1alpha1` API group, validated at admission with the gateway's own validation code, and rendered into gateways that are still installed with the existing `featherbit-gateway` Helm chart. A `FeatherbitGateway` object binds one gateway installation to the namespaces and labels it accepts resources from. One team's invalid object is excluded with a condition and never blocks another team's valid change.

```
Route/Policy/... CRs --watch--> operator --select+validate+compile--> sink
                                                                      |-- ConfigMap gateway.yaml --kubelet--> /etc/gateway --notify--> gateway hot-reload
                                                                      `-- etcd <prefix>/<kind>/<name> --2s poll--> every gateway replica
```

The operator owns configuration only. It never restarts pods, never touches `system.yaml`, and the chart stays the install path.

## Install

```bash
# 1. The operator (CRDs, controller, admission webhook)
helm install featherbit-operator oci://ghcr.io/featherbitplatform/charts/featherbit-operator \
  --namespace featherbit-system --create-namespace

# 2. A gateway that reads its gateway.yaml from the ConfigMap the operator will render
#    (needs gateway chart >= 0.16, or `develop` until it is released)
helm install edge oci://ghcr.io/featherbitplatform/charts/featherbit-gateway \
  --namespace gateway-system --create-namespace \
  --set config.gatewayConfigMap=edge-gateway-config

# 3. Bind it: accept resources from the same namespace and render into that ConfigMap
kubectl apply -f - <<YAML
apiVersion: featherbit.io/v1alpha1
kind: FeatherbitGateway
metadata: {name: edge, namespace: gateway-system}
spec:
  sink: {configMap: {name: edge-gateway-config}}
YAML
```

Then apply `Route` and `Policy` objects in `gateway-system` and watch them with `kubectl get featherbit -n gateway-system`. The documentation covers the rest: [getting started](https://featherbitplatform.github.io/gateway/operator/getting-started), the [CRD reference](https://featherbitplatform.github.io/gateway/operator/crds), [conditions](https://featherbitplatform.github.io/gateway/operator/conditions) and [releases](https://featherbitplatform.github.io/gateway/operator/releases), all under [featherbitplatform.github.io/gateway/operator](https://featherbitplatform.github.io/gateway/operator/).

## The CRDs are the source of truth

On an operator-managed gateway the custom resources win. Edits made through the gateway's Admin API, web UI or MCP tools are not written back to the resources and are overwritten by the next render (file mode) or the next reconcile (etcd mode). Use the UI to inspect and debug, not to edit.

## Sinks

- Each gateway needs its own sink. Two `FeatherbitGateway` objects that share a sink (the same `configMap.name` in one namespace, or a shared etcd endpoint with equal or nested prefixes such as `/fb` and `/fb/b`) would overwrite each other. The older gateway (creationTimestamp, then namespace/name) keeps writing; the newer one reports `Ready=False` / `InvalidSpec` naming the other gateway, writes nothing and emits a warning event.
- The sink is written when the rendered config changes, when `spec.sink` changes (`status.sinkFingerprint`), and to repair drift: a deleted or edited ConfigMap is restored, and an etcd sink is re-applied every 10 minutes.
- A legitimately empty selection renders an empty config and empties the sink (ConfigMap data, or the etcd prefix). That is by design. Selector mistakes (unknown operators, `In` without values, invalid label syntax) are rejected by the admission webhook, so they cannot silently empty a sink.

## Upgrades

`helm upgrade` does not update the contents of a chart's `crds/` directory. On every operator upgrade apply the CRDs yourself:

```bash
kubectl apply -f charts/featherbit-operator/crds/
# or, from the installed binary:
featherbit-operator crds | kubectl apply -f -
```

## Trust boundary

Whoever can create a `FeatherbitGateway` in a namespace can direct the operator to read the `user` and `password` keys of any Secret in that namespace (etcd `credentialsSecretRef`) and to overwrite the `gateway.yaml` key of any ConfigMap there (`sink.configMap.name`). Restrict `FeatherbitGateway` create/update rights with RBAC accordingly.

## Artifacts

| Artifact | Location |
|---|---|
| Image | `featherbit/operator` on Docker Hub (multi-arch, `FROM scratch`, non-root) |
| Helm chart | `oci://ghcr.io/featherbitplatform/charts/featherbit-operator` |
| Helm chart (mirror) | `oci://registry-1.docker.io/featherbit/featherbit-operator` |
| SBOMs | CycloneDX, one for the crate and one for the image, attached to each [GitHub release](https://github.com/featherbitplatform/gateway-operator/releases) |

Run an operator at least as new as the gateways it serves; operator minor versions track gateway minor versions. See the [releases page](https://featherbitplatform.github.io/gateway/operator/releases) for the coupling rules.

## Design

The architecture, condition tables, reconcile flow and version-coupling rules are in [`docs/superpowers/specs/2026-10-08-operator-design.md`](docs/superpowers/specs/2026-10-08-operator-design.md). Build and test commands for contributors are in [`CLAUDE.md`](CLAUDE.md).

## License

See [`LICENSE`](LICENSE).
