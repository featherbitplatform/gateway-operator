# featherbit gateway-operator — v1 design

Date: 2026-10-08
Status: approved design, pending implementation plan
Gateway version studied: 0.15.0 (`featherbitplatform/gateway`, branch `develop`)

## 1. Intent

The Helm chart `featherbit-gateway` is published. The next step on the gateway
roadmap is a Kubernetes operator in this repository. Its first release makes
the gateway's configuration a set of Kubernetes resources: routes, policies and
the shared kinds become namespaced custom resources, validated at admission
with the gateway's own validation code, and reconciled into gateways that are
still installed with the existing chart.

Decisions taken during brainstorming (user choices, in order):

| Question | Decision |
|---|---|
| v1 purpose | **Config as CRDs.** The chart stays the install path; the operator owns configuration only. |
| Config delivery | **Render a ConfigMap** for file-mode installs; **reconcile the etcd prefix directly** for etcd-mode installs. Admin API push deferred. |
| Stack | **Rust + kube-rs**, validation reused **in-process** by giving the gateway crate a library target. |
| Binding | **Gateway-side selector**: a `FeatherbitGateway` object declares which namespaces/labels it accepts resources from. Resources never name a gateway. |
| CRD shape | **One CRD per `gateway.yaml` kind**, schemas generated from the gateway's serde types, plus the binding kind. |
| Invalid objects | **Per-object exclusion** with conditions, not all-or-nothing. |

Assumption to confirm before implementation: the API group is `featherbit.io`.
It must be a domain the organization controls.

## 2. Goals and non-goals

Goals

- `kubectl apply` of a `Policy` with an unwired port, an unknown node type or a
  malformed graph fails at admission with the same message the gateway's Admin
  API would return.
- A platform team runs one or more chart-installed gateways and chooses, per
  gateway, which namespaces may contribute routes and policies.
- One team's invalid object never blocks another team's valid change.
- Works unchanged against file-mode and etcd-mode gateway installs.
- The rendered configuration is inspectable with `kubectl` (ConfigMap) or
  `etcdctl` (prefix), and its hash is visible in status.
- Same security profile and release mechanics as the gateway: `FROM scratch`,
  nonroot, read-only filesystem, Helm chart published as OCI on release tags.

Non-goals for v1 (each is a candidate for a later spec)

- Gateway API (`GatewayClass`/`Gateway`/`HTTPRoute`) implementation.
- Operator-managed gateway Deployments, Services or Secrets ("full lifecycle").
- Pushing configuration through the Admin API.
- `secretRef` fields on stores or consumers; `${ENV}` placeholders pass through
  exactly as the chart does today.
- An `Applied` condition read back from running gateways.
- Leader election / multiple operator replicas.
- Conversion webhooks (only `v1alpha1` exists).

## 3. Architecture

Three pieces across two repositories.

```
featherbitplatform/gateway                      featherbitplatform/gateway-operator
├── src/lib.rs      (new: config, graph,         ├── src/main.rs        one binary:
│                    state::validate_*, etcd     │                      controller + webhook + `crds`
│                    prefix reconcile, port_spec) │                      subcommand
├── src/main.rs     (thin bin over the lib)      ├── src/crd/           kube-derive wrappers over
└── charts/featherbit-gateway                    │                      gateway config types
      values: config.gatewayConfigMap (new)     ├── src/reconcile/     select → exclude → compile → sink
                                                 ├── src/webhook/       AdmissionReview handlers
                                                 ├── src/sink/          configmap.rs, etcd.rs
                                                 └── charts/featherbit-operator
```

Data flow at runtime:

```
Route/Policy/... CRs ──watch──▶ operator ──select+validate+compile──▶ sink
                                                                      ├── ConfigMap gateway.yaml ──kubelet──▶ /etc/gateway ──notify──▶ gateway hot-reload
                                                                      └── etcd <prefix>/<kind>/<name> ──2s poll──▶ every gateway replica
```

### 3.1 Gateway-side changes (separate PR in the gateway repo)

1. **Library target.** Add `src/lib.rs` exposing the modules the operator
   needs; `src/main.rs` keeps only CLI parsing and process wiring. Public
   surface required by the operator:
   - `config::{GatewayConfig, RouteConfig, MatchRule, PolicyConfig, NodeConfig,
     EdgeConfig, SupernodeConfig, PluginConfigDef, StoreConfig, StoreTlsConfig}`
     and `consumers::ConsumerConfig`, all deriving `schemars::JsonSchema`
     (schemars 1 is already a dependency for the MCP tools).
   - `graph::{validate_policy, validate_supernode, prepare_policy}`.
   - `routing::validate_match_rule`.
   - `stores::validate_stores`, `consumers::ConsumerStore::from_config`.
   - `state::validate_gateway_config` (whole-config compile without runtime state).
   - `plugins::port_spec` (a `None` result means an unknown node type).
   - `config_store::etcd`: a new `reconcile_prefix(cfg: &EtcdConfig, desired:
     &GatewayConfig) -> Result<(), String>` that performs the existing
     put-desired-then-delete-stale sequence of `EtcdStore::commit` without
     requiring a `SharedState`. `commit` is refactored to call it after
     validation; behavior is unchanged.
   The crate name stays `featherbit`. The operator depends on it by git tag.
2. **Chart value `config.gatewayConfigMap: ""`.** When set, the chart's
   ConfigMap renders `system.yaml` only, and the projected `config` volume adds
   the named ConfigMap's `gateway.yaml` key. `config.gateway` / `gatewayRaw`
   are ignored with a NOTES warning when both are given. Documented in the
   chart README under a new "Operator-managed configuration" heading.

## 4. Custom resources

Group `featherbit.io`, version `v1alpha1`, all namespaced, category
`featherbit` (`kubectl get featherbit -A` lists every kind).

### 4.1 Resource kinds

| Kind | `spec` = gateway struct | Per-object validation at admission and reconcile |
|---|---|---|
| `Route` | `RouteConfig` minus `name` | `validate_match_rule`; `policy` non-empty |
| `Policy` | `PolicyConfig` minus `name` | `validate_policy`; every `node.type` has a `port_spec` or is `supernode`; standalone compile when self-contained (§6) |
| `Supernode` | `SupernodeConfig` minus `name` | `validate_supernode`; node types known |
| `PluginConfig` | `PluginConfigDef` minus `name` | `type` has a `port_spec` |
| `Store` | `StoreConfig` minus `name` | `validate_stores(&[store])` |
| `Consumer` | `ConsumerConfig` minus `name` | `ConsumerStore::from_config(&[consumer])` |

`metadata.name` becomes the gateway-level `name`. The CRD schema is generated
from the gateway types with `kube-derive` + schemars; opaque plugin `config`
maps are marked `x-kubernetes-preserve-unknown-fields`. Everything else is
structural so the API server prunes typos in known fields and the webhook's
strict deserialization rejects unknown ones.

A policy is therefore written exactly as in the gateway docs:

```yaml
apiVersion: featherbit.io/v1alpha1
kind: Policy
metadata: {name: api, namespace: shop}
spec:
  error_handler: on-error
  nodes:
    - {id: listener, type: listener}
    - {id: backend, type: upstream, config: {targets: [{host: ${UPSTREAM_HOST}, port: 80}]}}
    - {id: on-error, type: error-handler, config: {status_code: 502}}
    - {id: client, type: client}
  edges:
    - {from: listener.out, to: backend.in}
    - {from: backend.success, to: client.in}
    - {from: backend.error, to: on-error.in}
    - {from: on-error.success, to: client.in}
```

**Names are flat per gateway**, as in `gateway.yaml`. `spec.policy: api` refers
to whichever selected `Policy` is named `api` in any admitted namespace. The
operator never rewrites references, including those inside opaque plugin
config (`store:`, `config_ref`). Two selected objects of the same kind and
name are a **conflict**: the one with the oldest `creationTimestamp` wins
(ties broken by namespace/name order); the loser is excluded with
`Accepted=False`, reason `Conflicted`, message naming the winner.

**Secrets** stay `${ENV}` / `${ENV:-default}` placeholders. They are rendered
verbatim and resolve inside the gateway pod from `extraEnv` / `extraEnvFrom`,
identical to the chart's behavior. Store `password`, consumer credentials and
upstream targets all follow this rule.

Printer columns: `Route` shows `POLICY`, `PATH`, `PROGRAMMED`, `AGE`; the other
resource kinds show `ACCEPTED`, `PROGRAMMED`, `AGE`.

### 4.2 `FeatherbitGateway` (binding)

```yaml
apiVersion: featherbit.io/v1alpha1
kind: FeatherbitGateway
metadata: {name: edge, namespace: gateway-system}
spec:
  sink:                                 # exactly one of:
    configMap:
      name: edge-gateway-config         # created/owned by the operator in this namespace
    etcd:
      endpoints: ["http://etcd:2379"]
      prefix: /featherbit               # default
      timeoutMs: 3000                   # default
      credentialsSecretRef:             # optional; keys `user`, `password`,
        name: edge-etcd                 # same shape as the chart's config.etcd.existingSecret
  resources:
    namespaces:
      from: Same | All | Selector       # default Same
      selector: {matchLabels: {team: shop}}   # required when from: Selector
    selector: {matchLabels: {gateway: edge}}  # optional label selector on the objects
```

`spec.sink.etcd` mirrors the gateway's `EtcdConfig` (`endpoints`, `prefix`,
`user`/`password`, `timeout_ms`) so the same values the chart receives can be
copied over. Printer columns: `READY`, `ROUTES`, `POLICIES`, `HASH`, `AGE`.

### 4.3 Status and conditions

Resource kinds (`status.conditions`, `status.observedGeneration`,
`status.gateways: [{name, namespace}]`):

| Type | True | False reasons |
|---|---|---|
| `Accepted` | passes its per-object validation, is not a conflict loser, and (policies) compiles against the selecting gateway's shared objects | `Invalid` (message = validator output), `Conflicted`, `CompileFailed` (policies only; message = compiler output, covers unknown `store:` names and bad plugin config) |
| `ResolvedRefs` | every reference resolves inside the selecting gateway's set | `PolicyNotFound`, `PluginConfigNotFound`, `SupernodeNotFound` |
| `Programmed` | included in at least one gateway's rendered config | `NotSelected`, `Excluded` (Accepted or ResolvedRefs is False), `GatewayNotReady` |

`ResolvedRefs` and `Programmed` are evaluated per selecting gateway; an object
selected by two gateways reports the worst result and names both in
`status.gateways`. An object selected by no gateway has only `Programmed=False`
/ `NotSelected`.

`FeatherbitGateway` (`status.conditions`, `status.observedGeneration`,
`status.configHash`, `status.lastRenderedAt`, `status.counts: {routes,
policies, supernodes, pluginConfigs, stores, consumers, excluded}`):

| Type | True | False reasons |
|---|---|---|
| `Ready` | last reconcile rendered and wrote the sink | `InvalidSpec`, `CompileFailed` (message = compiler output), `SinkUnavailable` |

Status patches are server-side applies under field manager
`featherbit-operator` and are skipped when nothing changed, to keep API churn
proportional to real changes.

## 5. Reconcile flow

One controller keyed on `FeatherbitGateway`. Watches on the six resource
kinds, on `Namespace` (label changes affect `from: Selector`), and on
ConfigMaps labelled `app.kubernetes.io/managed-by=featherbit-operator`
(re-render if the sink is deleted) enqueue **every** `FeatherbitGateway`;
a cluster has a handful, and mapping an object back to the exact gateways
that select it buys nothing.

Per reconcile of gateway G:

1. **Select.** Resolve admitted namespaces from `spec.resources.namespaces`,
   list the six kinds there from the informer caches, apply
   `spec.resources.selector`.
2. **Resolve conflicts.** Group by kind and name; oldest wins; losers excluded.
3. **Validate per object** with the lib validators listed in §4.1. Failures
   are excluded with `Accepted=False`.
4. **Resolve references** against the surviving set: route → policy; node
   `config_ref` → plugin config; `type: supernode` instance → supernode.
   Failures are excluded with `ResolvedRefs=False`.
5. **Compile each policy on its own** against the surviving shared objects:
   a `GatewayConfig` holding that one policy plus all surviving supernodes,
   plugin configs, stores and consumers, passed to `validate_gateway_config`.
   This is where plugin config errors, unwired ports after supernode
   expansion, and `store:` names that resolve to nothing are caught, without
   parsing opaque config. A failing policy is excluded with `Accepted=False` /
   `CompileFailed` and the compiler's message verbatim. A route whose policy
   was excluded for any reason is excluded with `Programmed=False` / `Excluded`.
6. **Compile the whole surviving set** with `validate_gateway_config`. This
   is the gateway's own load-time check and the final gate. If it fails (a gap
   between per-policy compiles and the full compile, e.g. a duplicate route
   match), nothing is written, the last rendered config stays, `Ready=False` /
   `CompileFailed` carries the message, and an Event is recorded on G.
7. **Render** the `GatewayConfig` to YAML with the lib's serde types so the
   file is byte-identical to what the Admin API's export would produce for the
   same config. Compute `configHash` (sha256 of the YAML).
8. **Write the sink** only if the hash differs from `status.configHash` or the
   sink content was observed to drift (§7).
9. **Patch status** on G and on every selected object, only where changed.
10. Requeue: on success every 10 minutes as a drift safety net; on sink failure
    with exponential backoff capped at 5 minutes.

The operator is **not** the gateway's lifecycle owner: it never restarts pods,
never touches `system.yaml`, and deleting a `FeatherbitGateway` leaves the
last rendered ConfigMap or etcd prefix in place (no owner reference, no
finalizer). This is deliberate: deleting the binding must not break the next
pod restart of a running gateway. The ConfigMap carries
`app.kubernetes.io/managed-by=featherbit-operator` and an annotation naming
the gateway that rendered it, so cleanup is a documented manual step.

## 6. Admission webhook

A `ValidatingWebhookConfiguration` for CREATE and UPDATE on all seven kinds,
`failurePolicy: Fail`, `sideEffects: None`, `matchPolicy: Equivalent`, served
by axum over rustls on port 9443 inside the operator process at
`/validate/featherbit.io/v1alpha1/<kind>` (kind lowercase: the API server rejects uppercase path segments).

What the webhook judges (object alone, no cluster reads):

- Strict deserialization of `spec` into the gateway type (unknown fields in
  known structures rejected; opaque plugin `config` maps accepted as-is).
- The per-object validators of §4.1.
- `Policy` only: if the policy has no `config_ref` on any node and no
  `type: supernode` instance, it is self-contained and is **compiled
  standalone** via `validate_gateway_config` on a `GatewayConfig` holding just
  that policy, so unwired `success`/outcome ports and bad plugin config are
  rejected at `kubectl apply`. A policy that depends on shared objects is
  judged structurally only; its compile happens at reconcile and surfaces as
  `Accepted` / `ResolvedRefs`.
- `FeatherbitGateway`: exactly one sink set; `from: Selector` requires a
  selector; etcd `endpoints` non-empty; label selectors parse.

Rejection messages are the validator strings verbatim, prefixed with the
object's kind and name, so the `kubectl` error reads like the Admin API's 400.

Because the webhook can be bypassed (`failurePolicy` overrides, bulk restores,
objects created before the operator was installed), the reconcile loop re-runs
every check; the webhook only improves feedback latency.

**TLS.** The operator chart generates a CA and a serving certificate with
Helm's `genCA`/`genSignedCert`, stores them in a Secret kept across upgrades
with `lookup` (the same pattern the gateway chart uses for the admin
password), and injects `caBundle` into the webhook configuration.
`webhook.certManager.enabled=true` switches to a cert-manager `Certificate`
and the `cert-manager.io/inject-ca-from` annotation. The chart README repeats
the gateway chart's caveat: renderers without cluster access (ArgoCD, Flux in
template mode) must use cert-manager or supply the certificate.

## 7. Sinks

### 7.1 ConfigMap (file-mode gateways)

- Object `spec.sink.configMap.name` in the gateway's namespace, key
  `gateway.yaml`, written by server-side apply with `force: true` under field
  manager `featherbit-operator`. Labels: `app.kubernetes.io/managed-by`,
  `featherbit.io/gateway=<name>`.
- The gateway chart mounts it via `config.gatewayConfigMap`; the gateway's
  `notify` watcher reloads once the kubelet refreshes the projected volume
  (up to about a minute, as the chart already documents).
- Drift: the ConfigMap is in the watch set, so a manual edit or deletion is
  re-rendered on the next reconcile.
- Live Admin API / UI / MCP edits in file mode are in-memory and are
  overwritten by the gateway's next hot-reload. The operator docs state this
  as the rule: **CRDs are the source of truth; use the UI to inspect and
  debug, not to edit, on an operator-managed gateway.**

### 7.2 etcd (etcd-mode gateways)

- Confirmed behavior in gateway 0.15.0: with `config.source: etcd` the file
  watcher is never started; `gateway.yaml` only seeds an empty prefix on first
  boot, and `spawn_watch` re-reads the whole prefix every 2 s and applies it
  with last-good semantics. A ConfigMap would therefore never reach an
  etcd-mode gateway after its first boot.
- The operator calls the lib's `reconcile_prefix` with an `EtcdConfig` built
  from `spec.sink.etcd` and the referenced Secret. Key layout
  (`<prefix>/routes/<name>`, `/policies/`, `/consumers/`, `/supernodes/`,
  `/plugin_configs/`, `/stores/`, one JSON document each) and the write
  sequence are the gateway's own code, so they cannot drift.
- Every replica converges within one poll interval. Admin API edits in etcd
  mode are durable but are likewise overwritten on the next reconcile.
- Drift: the 10-minute requeue re-applies the prefix. Reading the prefix back
  to detect drift sooner is a possible later addition.
- The chart's seed `gateway.yaml` should be empty for an operator-managed
  etcd gateway; if it is not, the first reconcile replaces whatever it seeded.
  Documented in the chart README.

## 8. Packaging

- Binary `featherbit-operator`: `run` (default; controller + webhook +
  metrics/health server), `crds` (prints generated CRD YAML, used by the chart
  build and by docs), `version`.
- Image `featherbit/featherbit-operator`, `FROM scratch`, uid 65532, read-only
  root, same CI scanners as the gateway (`dev/sast` equivalents: cargo audit,
  clippy, grype/trivy on the image, trivy config on the chart).
- Chart `charts/featherbit-operator`: CRDs (in `crds/`, so Helm installs them
  before templates and never deletes them), Deployment (1 replica),
  ServiceAccount + ClusterRole (get/list/watch on the seven kinds and
  Namespaces; patch on their `status`; get/list/watch/create/patch ConfigMaps;
  get Secrets referenced by sinks; create Events), Service for the webhook,
  `ValidatingWebhookConfiguration`, optional ServiceMonitor, optional
  cert-manager resources. Published as OCI to GHCR and Docker Hub on release
  tags like the gateway chart.
- Versioning: operator and chart share a version; releases track the gateway's
  minor version (§10).
- Repo conventions: Gitflow (`develop`, `feature/*`, `release/*`), Conventional
  Commits, no `Co-Authored-By` trailers; a `CLAUDE.md` is added with the
  operator's build/test commands when the scaffold lands.

## 9. Errors and observability

- Every failure is visible in one of three places: a webhook rejection, a
  condition on the object that caused it, or `Ready=False` on the gateway.
  Events are emitted on the object for exclusions (`Warning` / `Excluded`)
  and on the gateway for sink failures and compile failures.
- The operator never deletes a rendered config and never writes a config that
  failed the whole-config compile.
- Metrics (Prometheus, port 8080 `/metrics`, no auth, cluster-internal):
  `featherbit_operator_reconcile_total{gateway,result}`,
  `featherbit_operator_reconcile_duration_seconds{gateway}`,
  `featherbit_operator_excluded_objects{gateway,kind,reason}`,
  `featherbit_operator_rendered_config_info{gateway,hash}` (gauge 1),
  `featherbit_operator_webhook_requests_total{kind,allowed}`.
- `/healthz` (process up) and `/readyz` (informers synced, webhook cert
  loaded) on the same port, used by the chart's probes.
- Logs via `tracing`, `text` or `json`, level from `RUST_LOG` or a chart value,
  matching the gateway's conventions.

## 10. Version coupling

The operator pins one gateway lib version; its webhook knows that version's
node catalog and its CRD schemas are generated from that version's types.
Rules documented in the README:

- Run an operator version **at least as new** as the gateways it serves. An
  older operator rejects node types a newer gateway accepts.
- Operator minor versions track gateway minor versions (`0.16.x` operator ↔
  `0.16.x` gateway). Patch versions are independent.
- CRD schema changes within `v1alpha1` are additive only; a field removal
  requires a new API version.

## 11. Testing

- **Unit (cargo test)**: selection and namespace admission; conflict
  resolution ordering; exclusion and condition computation for every reason in
  §4.3; YAML rendering equivalence against the gateway's export for a fixture
  config; webhook handlers against `AdmissionReview` fixtures for every kind,
  accepted and rejected; standalone-compile detection for policies.
- **Lib contract tests (gateway repo)**: `reconcile_prefix` against the
  existing `FakeConfigStore`/etcd tests; a test that `validate_gateway_config`
  and the Admin API's `POST /api/policies/validate` agree on a shared fixture
  set, so the webhook's messages stay identical to the API's.
- **End-to-end (kind, CI matrix)**, following the gateway chart's existing
  kind job:
  1. Install the gateway chart with `config.gatewayConfigMap=edge-gateway-config`
     and an empty `config.gateway`; install the operator chart; apply a
     `FeatherbitGateway` with the ConfigMap sink and sample `Route`/`Policy`
     objects; assert the ConfigMap content, `Ready=True`, and a `200` through a
     port-forward to the data plane.
  2. Apply a policy with an unwired port and assert `kubectl apply` fails with
     the gateway's message; apply a route to a missing policy and assert
     `ResolvedRefs=False` while the other route stays `Programmed=True`.
  3. Same as 1 with a single-node etcd (`bitnami/etcd` or the upstream image)
     and the etcd sink, asserting convergence on two gateway replicas.
- **Chart**: `helm lint --strict` over every `ci/*-values.yaml`, trivy config
  scan, `helm test` hook checking `/readyz`.

## 12. Items carried into the plan

- Confirm the API group domain (`featherbit.io`) before the first CRD is
  generated; it is the only decision in this spec still awaiting confirmation.
- Pick the kube-rs release line that targets schemars 1 (the gateway's schemars
  major) and pin it; the lib split must not force a schemars downgrade.
- Implementation order: the gateway-side PR (lib target, `JsonSchema` derives,
  `reconcile_prefix`, chart value) lands and is tagged first; the operator
  depends on that tag.
- All six resource kinds, `Consumer` included, ship in v1.
