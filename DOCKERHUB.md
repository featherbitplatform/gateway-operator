# featherbit operator

Kubernetes operator for the [featherbit](https://hub.docker.com/r/featherbit/featherbit) API gateway. It makes the gateway's configuration a set of Kubernetes resources: routes, policies and the shared kinds (supernodes, plugin configs, stores, consumers) are custom resources in the `featherbit.io/v1alpha1` API group, validated at admission with the gateway's own validation code, and rendered into gateways installed with the `featherbit-gateway` Helm chart. Single static binary, `FROM scratch`, runs as non-root.

## Install

```bash
# 1. The operator (CRDs, controller, admission webhook)
helm install featherbit-operator oci://registry-1.docker.io/featherbit/featherbit-operator \
  --namespace featherbit-system --create-namespace

# 2. A gateway that reads gateway.yaml from the ConfigMap the operator renders
#    (config.gatewayConfigMap needs gateway chart >= 0.16)
helm install edge oci://registry-1.docker.io/featherbit/featherbit-gateway \
  --namespace gateway-system --create-namespace \
  --set config.gatewayConfigMap=edge-gateway-config

# 3. Bind it to the ConfigMap
kubectl apply -f - <<YAML
apiVersion: featherbit.io/v1alpha1
kind: FeatherbitGateway
metadata: {name: edge, namespace: gateway-system}
spec:
  sink: {configMap: {name: edge-gateway-config}}
YAML
```

The custom resources are the source of truth: edits made in the gateway's Admin API or UI are overwritten by the next render.

## Artifacts

| Artifact | Location |
|---|---|
| Image | `featherbit/operator` (this repository) |
| Helm chart | `oci://registry-1.docker.io/featherbit/featherbit-operator` |
| Helm chart (mirror) | `oci://ghcr.io/featherbitplatform/charts/featherbit-operator` |
| SBOMs | CycloneDX (crate and image), attached to each [GitHub release](https://github.com/featherbitplatform/gateway-operator/releases) |

Documentation: https://featherbitplatform.github.io/gateway/operator/ - source: https://github.com/featherbitplatform/gateway-operator
