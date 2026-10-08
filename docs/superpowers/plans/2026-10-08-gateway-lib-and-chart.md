# Gateway lib target + operator-managed ConfigMap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `featherbit` crate linkable as a library, give its config types JSON schemas, expose an etcd prefix reconciler that needs no runtime state, and let the Helm chart mount `gateway.yaml` from an external ConfigMap, so the operator (plan 2) can depend on a tagged gateway commit.

**Architecture:** The crate keeps its name and layout; `src/lib.rs` becomes the crate root declaring every module `pub`, and `src/main.rs` shrinks to CLI parsing and process wiring over `featherbit::*`. The etcd store's put-desired-then-delete-stale sequence moves into an inherent method reused by `commit`, the seeder and a new free function `reconcile_prefix`. The chart gains one value, `config.gatewayConfigMap`, that swaps the chart-rendered `gateway.yaml` for an external ConfigMap key in the projected volume.

**Tech Stack:** Rust 2021 (`cargo`), serde + schemars 1, Helm 3 (`helm template`, `helm lint --strict`).

**Spec:** `gateway-operator/docs/superpowers/specs/2026-10-08-operator-design.md` (§3.1 "Gateway-side changes", §7 "Sinks", §12).

**Repository:** all work happens in `featherbitplatform/gateway` (the sibling `gateway/` clone). The namespace folder is not a git repo: run git as `git -C gateway ...` or `cd gateway` first.

## Global Constraints

- Branch `feature/operator-lib` off `develop` (gateway `CLAUDE.md`, Gitflow). PR targets `develop`.
- Conventional Commits; **no `Co-Authored-By` trailer** (gateway `CLAUDE.md`).
- Every commit must pass: `cargo fmt --check`, `cargo test --locked`, `cargo clippy --all-targets --locked -- -D warnings`, and the headless variants `cargo test --locked --no-default-features --features redis-store` and `cargo clippy --all-targets --no-default-features --locked -- -D warnings` (`.github/workflows/ci.yml`).
- Chart: `helm lint --strict charts/featherbit-gateway` plus `-f` every file under `charts/featherbit-gateway/ci/` and `ci/template-only/` must pass. Rendered output with `config.gatewayConfigMap` **unset must be byte-identical** to today's.
- Crate name stays `featherbit`; `publish = false` stays. Version is **not** bumped (release commits do that).
- schemars stays at major 1 (`schemars = "1"` in `Cargo.toml`); nothing may pull in schemars 0.8.
- The gateway's behavior at runtime is unchanged by this plan; it is a packaging and surface change only.

## Review Focus

1. **`config.gatewayConfigMap` together with `config.source: etcd`.** The external file is only an etcd seed on first boot; the chart must still render, and NOTES must say the file seeds the prefix. Pinned in Task 4 step 2 (`etcd + external` template case).
2. **External ConfigMap absent when the pod starts** (operator installed after the gateway). The pod must start and serve no routes rather than crash-loop on a missing volume source. Pinned in Task 4 step 2 (`optional: true` assertion).
3. **Headless lib build** (`--no-default-features`). The public surface the operator uses (`config`, `graph`, `routing`, `state::validate_gateway_config`, `plugins::port_spec`, `config_store::etcd::reconcile_prefix`) must compile and test without `ui`/`mcp`. Pinned in Task 1 step 6 and Task 3 step 5.
4. **Empty desired config on the etcd sink.** `reconcile_prefix` with zero resources must empty the prefix (every stale key deleted) and not error. Pinned in Task 3 step 1 (`desired_kvs` of an empty config is empty, so every current key is stale).
5. **Opaque plugin config in generated schemas.** `NodeConfig.config`, `PluginConfigDef.config` and `ConsumerConfig.credentials` must schema to objects with free-form values, or the operator's CRDs would prune plugin settings on apply. Pinned in Task 2 step 1.

---

### Task 1: Library target

**Files:**
- Create: `src/lib.rs`
- Create: `tests/lib_surface.rs`
- Modify: `src/main.rs` (whole file)
- Modify: `src/config/mod.rs:20-28` (drop the "binary crate" `#[allow(unused_imports)]` comment and attributes)

**Interfaces:**
- Consumes: nothing new.
- Produces: the crate `featherbit` as a library with every module `pub`: `featherbit::config::{GatewayConfig, RouteConfig, MatchRule, PolicyConfig, NodeConfig, EdgeConfig, Position, SupernodeConfig, PluginConfigDef, StoreConfig, StoreTlsConfig, EtcdConfig, load_yaml}`, `featherbit::consumers::{ConsumerConfig, ConsumerStore}`, `featherbit::graph::{validate_policy, validate_supernode, prepare_policy}`, `featherbit::routing::validate_match_rule`, `featherbit::stores::validate_stores`, `featherbit::state::validate_gateway_config`, `featherbit::plugins::port_spec`. All already exist with these names; this task only makes them reachable from outside the crate.

- [ ] **Step 1: Write the failing integration test**

`tests/` already holds non-Rust fixtures (`tests/config`, `tests/realms`, ...); Cargo only compiles `tests/*.rs`, so a new file there is safe.

```rust
// tests/lib_surface.rs
//! The public surface the gateway-operator links against. If this file stops
//! compiling, the operator breaks: extend it, never trim it.

use featherbit::config::{GatewayConfig, PolicyConfig};
use featherbit::graph::validate_policy;
use featherbit::plugins::port_spec;
use featherbit::routing::validate_match_rule;
use featherbit::state::validate_gateway_config;

const MINIMAL: &str = r#"
routes:
  - name: hello
    match: { path: /hello }
    policy: hello
policies:
  - name: hello
    nodes:
      - { id: listener, type: listener }
      - { id: mock, type: mocking, config: { response_example: "{}" } }
      - { id: client, type: client }
    edges:
      - { from: listener.out, to: mock.in }
      - { from: mock.success, to: client.in }
"#;

#[test]
fn whole_config_compile_accepts_minimal_config() {
    let gw: GatewayConfig = serde_yaml::from_str(MINIMAL).unwrap();
    validate_gateway_config(&gw).expect("minimal config compiles");
}

#[test]
fn whole_config_compile_rejects_route_to_missing_policy() {
    let gw: GatewayConfig =
        serde_yaml::from_str(&MINIMAL.replace("policy: hello", "policy: nope")).unwrap();
    let err = validate_gateway_config(&gw).unwrap_err();
    assert!(err.contains("nope"), "error names the missing policy: {err}");
}

#[test]
fn policy_validator_reports_duplicate_node_ids() {
    let policy: PolicyConfig = serde_yaml::from_str(
        r#"
name: dup
nodes:
  - { id: listener, type: listener }
  - { id: listener, type: listener }
  - { id: client, type: client }
edges:
  - { from: listener.out, to: client.in }
"#,
    )
    .unwrap();
    let errors = validate_policy(&policy).unwrap_err();
    assert!(errors.iter().any(|e| e == "Duplicate node id 'listener'"), "{errors:?}");
}

#[test]
fn match_rule_validator_rejects_bad_host_pattern() {
    let gw: GatewayConfig =
        serde_yaml::from_str(&MINIMAL.replace("{ path: /hello }", "{ path: /hello, host: '*' }"))
            .unwrap();
    assert!(validate_match_rule(&gw.routes[0].match_rule).is_err());
}

#[test]
fn port_spec_is_the_node_type_catalog() {
    assert!(port_spec("upstream").is_some());
    assert!(port_spec("listener").is_some());
    assert!(port_spec("no-such-node-type").is_none());
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --locked --test lib_surface`
Expected: compile error `unresolved import featherbit` / `can't find crate for featherbit` (there is no library target yet).

- [ ] **Step 3: Create `src/lib.rs` and shrink `src/main.rs`**

`src/lib.rs` takes the crate docs and the module list from `main.rs`, every module `pub`:

```rust
//! # featherbit
//!
//! A high-performance API gateway delivered as a single Rust binary.
//!
//! Featherbit routes traffic through **node-graph policies** declared in YAML:
//! each policy is a pipeline of nodes wired together by success/error ports.
//! Plugins come in two tiers — native Rust plugins (proxying, auth,
//! rate-limiting, CORS, logging, ...) plus scripted plugins written in Lua.
//! A [`context::Context`] object (`request`, `response`, `message`, `errors`)
//! flows through every node in the pipeline. Operations are handled by an
//! admin REST API with an embedded React UI, configuration hot-reload via a
//! file watcher, and Prometheus metrics per route and per node.
//!
//! # Architecture
//!
//! Request flow: HTTP request → `server::listener` matches a route → builds a
//! `Context` → `CompiledGraph::execute()` walks the policy's nodes following
//! success/error ports → the final `Context.response` is sent to the client.
//!
//! Configuration lives in two files: `system.yaml` (listeners, timeouts,
//! admin API, logging) and `gateway.yaml` (routes and policies). Both support
//! `${ENV_VAR:-default}` interpolation and the latter is hot-reloaded on change.
//!
//! # Library use
//!
//! The binary in `src/main.rs` is a thin CLI over this crate. External
//! consumers (the Kubernetes operator in `featherbitplatform/gateway-operator`)
//! link the crate for its config types and validators:
//! [`config`], [`graph`], [`routing`], [`stores`], [`consumers`],
//! [`state::validate_gateway_config`], [`plugins::port_spec`] and
//! [`config_store::etcd::reconcile_prefix`]. `tests/lib_surface.rs` pins that
//! surface.

// `PluginExecutionError` deliberately carries the whole `Context` by value so the
// graph engine can route a failing node's context out through its `error` port
// (see `plugins::PluginExecutionError`). That makes the `Err` variant large by
// design; boxing it would ripple through the `Plugin` trait and every plugin.
#![allow(clippy::result_large_err)]

pub mod acme;
pub mod admin;
pub mod balancer;
pub mod batch;
pub mod config;
pub mod config_store;
pub mod consumers;
pub mod context;
pub mod debug;
pub mod graph;
pub mod hot_reload;
pub mod mcp;
pub mod metrics;
pub mod net;
pub mod outbound;
pub mod plugins;
pub mod ratelimit;
pub mod routing;
pub mod server;
pub mod sessions;
pub mod state;
pub mod stores;
pub mod stream;
#[cfg(test)]
pub(crate) mod test_log;
pub mod traffic;
pub mod vars;
```

`src/main.rs` keeps the allocator, `Cli`, `main`, `shutdown_signal`, `build_etcd_source`, `spawn_etcd_watch` and `init_logging` exactly as they are today, with only the header changed: delete the crate doc comment, the `#![allow(clippy::result_large_err)]` line and every `mod x;` line, and replace the `use crate::...` imports with:

```rust
use featherbit::config::{self, ConfigSourceKind, GatewayConfig, SystemConfig};
use featherbit::config_store::{self, ConfigStore, FileConfigStore};
use featherbit::state::SharedState;
use featherbit::{admin, hot_reload, server, stream};
```

Keep this as the first lines of `main.rs`:

```rust
//! Command-line entry point: parses `--system-config` / `--gateway-config`,
//! loads both files, and runs the gateway (`featherbit` crate).

/// mimalloc instead of the platform allocator. The published image is a static
/// musl build, and musl's malloc serializes under concurrency: the competitive
/// benchmark measured ~2x throughput at 4 cores (~3x on 64 KiB payloads).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

In `src/config/mod.rs`, delete the three `#[allow(unused_imports)]` attributes and the two-line comment above them ("featherbit is a binary crate, so `pub` exports nothing externally ..."): the re-exports are now genuinely public.

- [ ] **Step 4: Build and fix visibility**

Run: `cargo build --locked`
Expected: either clean, or `private type in public interface` / `field is private` errors on the handful of items `main.rs` reaches through (`state.resources.traffic.cache.set_capacity`, `state.routes`, `route.match_rule.path`). Fix each by making that field or function `pub` (never by moving code back into `main.rs`). Repeat until clean.

- [ ] **Step 5: Run the new and the existing tests**

Run: `cargo test --locked --test lib_surface` then `cargo test --locked`
Expected: 5 passed in `lib_surface`; the full suite passes with the same count as on `develop` plus 5.

- [ ] **Step 5b: Pin that the Admin API and the library validator agree**

The operator's webhook calls `validate_gateway_config` in-process while the web UI calls `POST /api/policies/validate`; the two must give the same verdict for the same policy. Add to the existing `tests` module in `src/admin/policies.rs` (next to `test_validate_reports_forced_buffering`, reusing its `validate_policy_json` helper):

```rust
    /// The operator's admission webhook validates in-process with
    /// `validate_gateway_config`; the UI uses this endpoint. Same policy,
    /// same verdict, or `kubectl apply` and the UI would disagree.
    #[tokio::test]
    async fn test_validate_endpoint_agrees_with_validate_gateway_config() {
        let policy = serde_json::json!({
            "name": "cors",
            "nodes": [
                { "id": "listener", "type": "listener" },
                { "id": "cors", "type": "cors", "config": { "allow_origins": "*" } },
                { "id": "client", "type": "client" }
            ],
            "edges": [
                { "from": "listener.out", "to": "cors.in" },
                { "from": "cors.success", "to": "client.in" }
            ]
        });
        let api = validate_policy_json(policy.clone()).await;
        assert_eq!(api["valid"], false, "{api}");
        let api_errors: Vec<String> = serde_json::from_value(api["errors"].clone()).unwrap();

        let mut gw: crate::config::GatewayConfig = serde_yaml::from_str("{}").unwrap();
        gw.policies.push(serde_json::from_value(policy).unwrap());
        let lib_error = crate::state::validate_gateway_config(&gw).unwrap_err();
        assert!(
            api_errors.iter().any(|e| lib_error.contains(e.as_str())),
            "API errors {api_errors:?} are not reported by the lib: {lib_error}"
        );
        assert!(lib_error.contains("preflight"), "{lib_error}");
    }
```

Run: `cargo test --locked test_validate_endpoint_agrees_with_validate_gateway_config`
Expected: PASS. Add `src/admin/policies.rs` to the Step 7 commit.

- [ ] **Step 6: Lint in both feature configurations**

Run:
```bash
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --all-targets --no-default-features --locked -- -D warnings
cargo test --locked --no-default-features --features redis-store --test lib_surface
```
Expected: all clean. A lib exposes items the bin build used to flag as dead; if clippy now reports `unused` on an `#[allow(dead_code)]` attribute, remove that attribute.

- [ ] **Step 7: Commit**

```bash
git add src/lib.rs src/main.rs src/config/mod.rs tests/lib_surface.rs
git commit -m "refactor: split the crate into a library and a thin binary

src/lib.rs is now the crate root with every module public; src/main.rs
keeps only CLI parsing and process wiring. tests/lib_surface.rs pins the
surface the gateway-operator links against."
```

---

### Task 2: JSON schemas for the config types

**Files:**
- Modify: `src/config/gateway.rs:6-7` (imports) and every `#[derive(...)]` on lines 26, 55, 70, 126, 142, 166, 176, 193, 215, 236, 294
- Modify: `src/consumers/mod.rs:33` (derive on `ConsumerConfig`)
- Test: `src/config/gateway.rs` (existing `#[cfg(test)] mod tests` at the bottom of the file; add there)

**Interfaces:**
- Consumes: Task 1 (types are public).
- Produces: `schemars::JsonSchema` implemented for `GatewayConfig`, `RouteConfig`, `MatchRule`, `PolicyConfig`, `NodeConfig`, `Position`, `EdgeConfig`, `SupernodeConfig`, `PluginConfigDef`, `StoreConfig`, `StoreTlsConfig`, `ConsumerConfig`. The operator's `kube::CustomResource` derive requires exactly this trait on every `spec` type.

- [ ] **Step 1: Write the failing test**

Append to the `tests` module at the end of `src/config/gateway.rs`:

```rust
    /// The operator generates CRD schemas from these types; serde attributes
    /// must carry over and opaque plugin config must stay free-form.
    #[test]
    fn config_types_generate_json_schemas() {
        let policy = serde_json::to_value(schemars::schema_for!(PolicyConfig)).unwrap();
        assert!(policy["properties"]["nodes"].is_object());
        assert!(policy["properties"]["error_handler"].is_object());

        let node = serde_json::to_value(schemars::schema_for!(NodeConfig)).unwrap();
        assert!(node["properties"]["type"].is_object(), "serde rename honored: {node}");
        assert!(node["properties"]["node_type"].is_null());
        let required = node["required"].as_array().unwrap();
        assert!(required.iter().any(|v| v == "id") && required.iter().any(|v| v == "type"));
        // Opaque plugin config: an object whose values are unconstrained.
        let config = &node["properties"]["config"];
        assert_eq!(config["type"], "object");
        assert_ne!(config["additionalProperties"], false, "{config}");

        let route = serde_json::to_value(schemars::schema_for!(RouteConfig)).unwrap();
        assert!(route["properties"]["match"].is_object(), "{route}");

        let consumer =
            serde_json::to_value(schemars::schema_for!(crate::consumers::ConsumerConfig)).unwrap();
        assert_eq!(consumer["properties"]["credentials"]["type"], "object");

        // Every top-level kind the operator exposes as a CRD.
        for schema in [
            serde_json::to_value(schemars::schema_for!(SupernodeConfig)).unwrap(),
            serde_json::to_value(schemars::schema_for!(PluginConfigDef)).unwrap(),
            serde_json::to_value(schemars::schema_for!(StoreConfig)).unwrap(),
            serde_json::to_value(schemars::schema_for!(GatewayConfig)).unwrap(),
        ] {
            assert_eq!(schema["type"], "object", "{schema}");
        }
    }
```

If `src/config/gateway.rs` has no `#[cfg(test)] mod tests` block, add one at the end of the file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // (test above goes here)
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --locked config_types_generate_json_schemas`
Expected: compile error `the trait bound PolicyConfig: JsonSchema is not satisfied`.

- [ ] **Step 3: Add the derives**

In `src/config/gateway.rs` change the import block to:

```rust
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
```

and add `JsonSchema` to each of the eleven derives, e.g.:

```rust
#[derive(Debug, Default, Deserialize, Serialize, Clone, JsonSchema)]
pub struct GatewayConfig {
```

```rust
#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema)]
pub struct RouteConfig {
```

Same change on `MatchRule`, `PolicyConfig`, `NodeConfig`, `Position`, `EdgeConfig`, `SupernodeConfig`, `PluginConfigDef`, `StoreConfig`, `StoreTlsConfig`.

In `src/consumers/mod.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ConsumerConfig {
```

`#[serde(rename)]`, `#[serde(default = "...")]` and `skip_serializing_if` are all understood by schemars 1; no schemars-specific attributes are needed.

- [ ] **Step 4: Run the test**

Run: `cargo test --locked config_types_generate_json_schemas`
Expected: PASS. If `additionalProperties` asserts fail, the `HashMap<String, serde_json::Value>` field needs `#[schemars(with = "std::collections::HashMap<String, serde_json::Value>")]` removed/avoided; schemars 1 maps `serde_json::Value` to `true` (any), which is what the CRD needs.

- [ ] **Step 5: Full checks and commit**

Run: `cargo fmt --check && cargo test --locked && cargo clippy --all-targets --locked -- -D warnings`
Expected: clean.

```bash
git add src/config/gateway.rs src/consumers/mod.rs
git commit -m "feat(config): derive JsonSchema on the gateway.yaml resource types

The operator generates its CRD OpenAPI schemas from these types."
```

---

### Task 3: `reconcile_prefix` for the etcd store

**Files:**
- Modify: `src/config_store/etcd.rs:216-251` (`write_all`), `:258-316` (`commit`), new items after `impl EtcdConfigStore`
- Test: `src/config_store/etcd.rs` (existing `#[cfg(test)] mod tests` at line ~500)

**Interfaces:**
- Consumes: `EtcdConfigStore::{new, range_prefix, put, delete, authenticate, route_key, policy_key, consumer_key, supernode_key, plugin_config_key, store_key}` and `gateway_from_kvs` (all existing, private).
- Produces:
  - `pub async fn featherbit::config_store::etcd::reconcile_prefix(cfg: &EtcdConfig, desired: &GatewayConfig) -> Result<(), String>` — makes the prefix hold exactly `desired`. The operator's etcd sink calls this and nothing else.
  - private `EtcdConfigStore::desired_kvs(&self, gw: &GatewayConfig) -> Vec<(String, Vec<u8>)>` and `async fn reconcile(&self, desired: &GatewayConfig) -> Result<(), String>`.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `src/config_store/etcd.rs`:

```rust
    fn store_at(prefix: &str) -> EtcdConfigStore {
        let cfg: EtcdConfig = serde_yaml::from_str(&format!(
            "endpoints: ['http://127.0.0.1:2379']\nprefix: {prefix}\n"
        ))
        .unwrap();
        EtcdConfigStore::new(&cfg)
    }

    fn sample_config() -> GatewayConfig {
        serde_yaml::from_str(
            r#"
routes:
  - { name: r1, match: { path: /a }, policy: p1 }
policies:
  - name: p1
    nodes: [{ id: listener, type: listener }, { id: client, type: client }]
    edges: [{ from: listener.out, to: client.in }]
consumers:
  - { name: c1, credentials: { key-auth: { key: k } } }
supernodes:
  - { name: s1, nodes: [{ id: input, type: input }, { id: output, type: output }], edges: [{ from: input.out, to: output.in }] }
plugin_configs:
  - { name: pc1, type: cors, config: { allow_origins: "*" } }
stores:
  - { name: st1, type: redis, url: "redis://r:6379" }
"#,
        )
        .unwrap()
    }

    /// desired_kvs is the write side of gateway_from_kvs: one JSON document
    /// per resource under the six key families, and reading them back yields
    /// the same config.
    #[test]
    fn desired_kvs_round_trips_through_gateway_from_kvs() {
        let store = store_at("/featherbit");
        let gw = sample_config();
        let kvs = store.desired_kvs(&gw);
        let keys: Vec<&str> = kvs.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            [
                "/featherbit/routes/r1",
                "/featherbit/policies/p1",
                "/featherbit/consumers/c1",
                "/featherbit/supernodes/s1",
                "/featherbit/plugin_configs/pc1",
                "/featherbit/stores/st1",
            ]
        );
        let back = gateway_from_kvs("/featherbit", kvs).unwrap();
        assert_eq!(
            serde_json::to_value(&back).unwrap(),
            serde_json::to_value(&gw).unwrap()
        );
    }

    /// An empty desired config produces no keys, so reconcile deletes every
    /// current key: the operator uses this to empty a gateway's prefix.
    #[test]
    fn desired_kvs_of_empty_config_is_empty() {
        let store = store_at("/featherbit/");
        let empty: GatewayConfig = serde_yaml::from_str("{}").unwrap();
        assert!(store.desired_kvs(&empty).is_empty());
    }

    /// The prefix's trailing slash is normalized by `new`, so keys never
    /// double up a separator.
    #[test]
    fn desired_kvs_normalizes_prefix() {
        let store = store_at("/fb/");
        let kvs = store.desired_kvs(&sample_config());
        assert_eq!(kvs[0].0, "/fb/routes/r1");
    }
```

`EtcdConfig` and `GatewayConfig` are already imported at the top of the file (`use crate::config::{EtcdConfig, GatewayConfig, ...}`), so `use super::*;` covers them.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --locked config_store::etcd::tests::desired_kvs`
Expected: compile error `no method named desired_kvs`.

- [ ] **Step 3: Implement `desired_kvs`, `reconcile`, `reconcile_prefix`; reuse them in `write_all` and `commit`**

Replace the body of `write_all` (lines 216-251) and add the two methods, inside `impl EtcdConfigStore`:

```rust
    /// The key/value pairs `gw` maps to under this store's prefix: one JSON
    /// document per resource, under `routes/`, `policies/`, `consumers/`,
    /// `supernodes/`, `plugin_configs/` and `stores/`. The write-side mirror
    /// of [`gateway_from_kvs`].
    fn desired_kvs(&self, gw: &GatewayConfig) -> Vec<(String, Vec<u8>)> {
        let mut kvs = Vec::new();
        for r in &gw.routes {
            kvs.push((self.route_key(&r.name), serde_json::to_vec(r).unwrap()));
        }
        for p in &gw.policies {
            kvs.push((self.policy_key(&p.name), serde_json::to_vec(p).unwrap()));
        }
        for c in &gw.consumers {
            kvs.push((self.consumer_key(&c.name), serde_json::to_vec(c).unwrap()));
        }
        for s in &gw.supernodes {
            kvs.push((self.supernode_key(&s.name), serde_json::to_vec(s).unwrap()));
        }
        for pc in &gw.plugin_configs {
            kvs.push((self.plugin_config_key(&pc.name), serde_json::to_vec(pc).unwrap()));
        }
        for s in &gw.stores {
            kvs.push((self.store_key(&s.name), serde_json::to_vec(s).unwrap()));
        }
        kvs
    }

    /// Puts every resource of `gw` (no deletes). Used by the first-boot seeder.
    async fn write_all(&self, gw: &GatewayConfig) -> Result<(), String> {
        for (key, value) in self.desired_kvs(gw) {
            self.put(&key, &value).await?;
        }
        Ok(())
    }

    /// Makes the prefix hold exactly `desired`: puts every desired key, then
    /// deletes the keys under the prefix that `desired` no longer contains.
    /// Not transactional — a reader polling mid-way sees a superset or a
    /// subset, which the gateway's apply either accepts or rejects as a
    /// whole and retries on its next poll.
    async fn reconcile(&self, desired: &GatewayConfig) -> Result<(), String> {
        let current: std::collections::HashSet<String> = self
            .range_prefix()
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let mut kept = std::collections::HashSet::new();
        for (key, value) in self.desired_kvs(desired) {
            self.put(&key, &value).await?;
            kept.insert(key);
        }
        for stale in current.difference(&kept) {
            self.delete(stale).await?;
        }
        Ok(())
    }
```

`commit` becomes:

```rust
    async fn commit(&self, state: &SharedState, candidate: GatewayConfig) -> Result<(), String> {
        // 1. Reject invalid config before touching etcd (synchronous 400).
        state.validate_gateway(&candidate)?;
        // 2. Reconcile etcd to match the candidate.
        self.reconcile(&candidate).await?;
        // 3. Apply locally so the writing node reflects the change immediately;
        //    other nodes converge on their next poll. Idempotent with the poll.
        state.apply_gateway(candidate).await
    }
```

Add the free function right after `impl ConfigStore for EtcdConfigStore { ... }` (before `gateway_from_kvs`):

```rust
/// Makes the etcd prefix described by `cfg` hold exactly `desired`, with no
/// gateway runtime involved. The Kubernetes operator's etcd sink calls this so
/// the key layout and write sequence stay the gateway's own code. The caller
/// is responsible for validating `desired` first
/// ([`crate::state::validate_gateway_config`]); every gateway polling the
/// prefix re-validates on apply and keeps its last-good config on failure.
pub async fn reconcile_prefix(cfg: &EtcdConfig, desired: &GatewayConfig) -> Result<(), String> {
    let store = EtcdConfigStore::new(cfg);
    if store.auth.is_some() {
        store.authenticate().await?;
    }
    store.reconcile(desired).await
}
```

- [ ] **Step 4: Run the etcd tests**

Run: `cargo test --locked config_store::etcd`
Expected: the three new tests and all existing `test_gateway_from_kvs*` / `test_is_empty*` tests pass.

- [ ] **Step 5: Pin the public function in the surface test, lint, commit**

Append to `tests/lib_surface.rs`:

```rust
/// `reconcile_prefix` is the operator's etcd sink; it must stay callable
/// without a `SharedState`. An unreachable endpoint is the cheapest way to
/// prove the signature from outside the crate.
#[tokio::test]
async fn reconcile_prefix_is_callable_without_runtime_state() {
    let cfg: featherbit::config::EtcdConfig =
        serde_yaml::from_str("endpoints: ['http://127.0.0.1:1']\ntimeout_ms: 100\n").unwrap();
    let gw: GatewayConfig = serde_yaml::from_str(MINIMAL).unwrap();
    let err = featherbit::config_store::etcd::reconcile_prefix(&cfg, &gw)
        .await
        .unwrap_err();
    assert!(!err.is_empty());
}
```

Run:
```bash
cargo fmt --check
cargo test --locked --test lib_surface
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --all-targets --no-default-features --locked -- -D warnings
```
Expected: 6 passed in `lib_surface`; clippy clean.

```bash
git add src/config_store/etcd.rs tests/lib_surface.rs
git commit -m "feat(etcd): expose reconcile_prefix for out-of-process writers

Factors the put-desired-then-delete-stale sequence out of commit into
EtcdConfigStore::reconcile and a SharedState-free reconcile_prefix, so the
operator's etcd sink reuses the gateway's key layout instead of copying it."
```

---

### Task 4: Chart value `config.gatewayConfigMap`

**Files:**
- Modify: `charts/featherbit-gateway/values.yaml` (after `gatewayRaw`, line ~117)
- Modify: `charts/featherbit-gateway/values.schema.json` (`properties.config.properties`)
- Modify: `charts/featherbit-gateway/templates/configmap.yaml`
- Modify: `charts/featherbit-gateway/templates/deployment.yaml:136-149` (projected sources)
- Modify: `charts/featherbit-gateway/templates/NOTES.txt` (before the `Docs:` line)
- Create: `charts/featherbit-gateway/ci/template-only/operator-values.yaml`
- Create: `dev/chart-render-check.sh` (template assertions, also run by hand)

**Interfaces:**
- Consumes: nothing from earlier tasks (chart only).
- Produces: value `config.gatewayConfigMap` (string, default `""`). When non-empty: the chart ConfigMap has no `gateway.yaml` key; the pod's `config` projected volume adds `configMap: {name: <value>, optional: true, items: [{key: gateway.yaml, path: gateway.yaml}]}`. The operator's ConfigMap sink writes key `gateway.yaml` into a ConfigMap of that name in the gateway's namespace.

- [ ] **Step 1: Write the failing render check**

```bash
#!/usr/bin/env bash
# dev/chart-render-check.sh — asserts on `helm template` output for the
# config.gatewayConfigMap value. Run from the repo root; exits non-zero on
# the first failed assertion.
set -euo pipefail
CHART=charts/featherbit-gateway

fail() { echo "FAIL: $1" >&2; exit 1; }

# 1. Default render: chart ConfigMap carries gateway.yaml, no external source.
default=$(helm template fb "$CHART")
grep -q '^  gateway.yaml: |' <<<"$default" || fail "default render lacks gateway.yaml in the chart ConfigMap"
grep -q 'optional: true' <<<"$default" && fail "default render must not reference an external ConfigMap"

# 2. External ConfigMap: key gone from the chart ConfigMap, optional projected source present.
ext=$(helm template fb "$CHART" --set config.gatewayConfigMap=edge-gateway-config)
grep -q '^  gateway.yaml: |' <<<"$ext" && fail "external render still renders gateway.yaml in the chart ConfigMap"
grep -q '^  system.yaml: |' <<<"$ext" || fail "external render lost system.yaml"
grep -A4 'name: edge-gateway-config' <<<"$ext" | grep -q 'optional: true' || fail "external ConfigMap source is not optional"
grep -A6 'name: edge-gateway-config' <<<"$ext" | grep -q 'path: gateway.yaml' || fail "external source does not project gateway.yaml"

# 3. Works together with etcd mode (the file is only a seed there).
helm template fb "$CHART" --set config.gatewayConfigMap=edge-gateway-config --set config.source=etcd \
  --set 'config.etcd.endpoints[0]=http://etcd:2379' >/dev/null || fail "etcd + external ConfigMap does not render"

# 4. NOTES mention the external ConfigMap (`helm template` does not render
#    NOTES.txt; a dry-run install does, without cluster access).
helm install fb "$CHART" --dry-run=client --set config.gatewayConfigMap=edge-gateway-config \
  | grep -q 'mounted from ConfigMap edge-gateway-config' || fail "NOTES do not mention the external ConfigMap"

echo "chart render check: OK"
```

Make it executable: `git update-index --chmod=+x dev/chart-render-check.sh` after adding (Windows checkouts do not carry the bit).

- [ ] **Step 2: Run it to verify it fails**

Run: `bash dev/chart-render-check.sh`
Expected: `FAIL: external render still renders gateway.yaml in the chart ConfigMap`.

- [ ] **Step 3: Implement the value**

`values.yaml`, directly after the `gatewayRaw: ""` line:

```yaml
  # Mount gateway.yaml from a ConfigMap you manage yourself (or that the
  # featherbit operator renders) instead of config.gateway/gatewayRaw, which
  # are then ignored. The ConfigMap must live in the release namespace and hold
  # a `gateway.yaml` key. It is optional at pod start: until it exists the
  # gateway serves no routes and picks the file up when it appears. With
  # config.source=etcd the file only seeds an empty prefix on first boot.
  gatewayConfigMap: ""
```

`values.schema.json`, inside `properties.config.properties`, after `"gatewayRaw"`:

```json
      "gatewayConfigMap": { "type": "string" },
```

`templates/configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "featherbit-gateway.configMapName" . }}
  labels:
    {{- include "featherbit-gateway.labels" . | nindent 4 }}
data:
  system.yaml: |
    {{- include "featherbit-gateway.systemConfig" . | nindent 4 }}
  {{- if not .Values.config.gatewayConfigMap }}
  # In etcd mode this file only seeds an empty prefix on first boot.
  gateway.yaml: |
    {{- include "featherbit-gateway.gatewayConfig" . | nindent 4 }}
  {{- end }}
```

`templates/deployment.yaml`, the projected sources block becomes:

```yaml
            sources:
              - configMap:
                  name: {{ include "featherbit-gateway.configMapName" . }}
              {{- if .Values.config.gatewayConfigMap }}
              # gateway.yaml managed outside the chart (e.g. by the operator).
              # Optional so the pod starts before the ConfigMap exists; the
              # gateway hot-reloads the file once it appears.
              - configMap:
                  name: {{ .Values.config.gatewayConfigMap }}
                  optional: true
                  items:
                    - key: gateway.yaml
                      path: gateway.yaml
              {{- end }}
              {{- if .Values.config.scripts }}
```

`templates/NOTES.txt`, before the final `Docs:` line:

```
{{- if .Values.config.gatewayConfigMap }}

gateway.yaml is mounted from ConfigMap {{ .Values.config.gatewayConfigMap }}
(config.gateway / config.gatewayRaw are ignored). Until that ConfigMap exists
the gateway serves no routes{{ if eq .Values.config.source "etcd" }}; in etcd mode
the file only seeds an empty prefix on first boot{{ end }}.
{{- end }}
```

`ci/template-only/operator-values.yaml`:

```yaml
# Operator-managed configuration: gateway.yaml comes from an external
# ConfigMap, so the chart must not render one and the helm test hook must not
# expect the default /hello route.
config:
  gatewayConfigMap: edge-gateway-config
tests:
  dataPlanePath: ""
```

- [ ] **Step 4: Run the render check and lint**

Run:
```bash
bash dev/chart-render-check.sh
helm lint --strict charts/featherbit-gateway
for f in charts/featherbit-gateway/ci/*.yaml charts/featherbit-gateway/ci/template-only/*.yaml; do helm lint --strict charts/featherbit-gateway -f "$f"; done
helm template fb charts/featherbit-gateway > /tmp/before.yaml   # on develop, for the byte-identical check:
git stash && helm template fb charts/featherbit-gateway > /tmp/base.yaml && git stash pop && diff /tmp/base.yaml <(helm template fb charts/featherbit-gateway)
helm install fb charts/featherbit-gateway --dry-run -f charts/featherbit-gateway/ci/template-only/operator-values.yaml | grep -A3 'mounted from ConfigMap'
```
Expected: `chart render check: OK`; every lint clean; the `diff` is empty (default render unchanged); the dry-run prints the NOTES paragraph naming `edge-gateway-config`.

- [ ] **Step 5: Commit**

```bash
git add charts/featherbit-gateway dev/chart-render-check.sh
git update-index --chmod=+x dev/chart-render-check.sh
git commit -m "feat(helm): mount gateway.yaml from an external ConfigMap via config.gatewayConfigMap

Lets the operator (or any GitOps tool) own gateway.yaml while the chart keeps
system.yaml. The source is optional so the pod starts before the ConfigMap
exists."
```

---

### Task 5: Documentation

**Files:**
- Modify: `charts/featherbit-gateway/README.md:30-37` (Configuration section)
- Modify: `website/docs/guides/deployment.md:184` (after the hot-reload paragraph)
- Modify: `website/docs/reference/roadmap.md:30` (Kubernetes row)
- Modify: `CLAUDE.md` ("Key modules" list and the Helm chart bullet)

**Interfaces:** none (docs).

- [ ] **Step 1: Chart README**

Add a bullet to the Configuration list after the `config.scripts` bullet:

```markdown
- `config.gatewayConfigMap` mounts `gateway.yaml` from a ConfigMap you manage (or that the [featherbit operator](https://github.com/featherbitplatform/gateway-operator) renders) instead of `config.gateway`/`gatewayRaw`. The source is optional at pod start, so the gateway serves no routes until the ConfigMap exists and hot-reloads once it does. CRDs or your GitOps tool are then the source of truth: Admin UI edits are overwritten on the next render.
```

- [ ] **Step 2: Deployment guide**

After the paragraph ending "within about a minute." add:

```markdown
To hand `gateway.yaml` to an external owner, set `config.gatewayConfigMap` to the name of a ConfigMap (in the release namespace, key `gateway.yaml`): the chart then renders only `system.yaml` and projects that ConfigMap into `/etc/gateway`, optional so the pod starts before it exists. This is how the [featherbit operator](https://github.com/featherbitplatform/gateway-operator) feeds configuration to a chart-installed gateway. In etcd mode the file only seeds an empty prefix on first boot, so the operator writes the etcd prefix directly instead.
```

- [ ] **Step 3: Roadmap row**

In the Kubernetes row of the table, replace the trailing sentence `Planned, in a separate repository: a Kubernetes **operator** (CRDs for routes/policies with the gateway's own validation in an admission webhook, Gateway API implementation).` with:

```markdown
**Operator in development** in [`gateway-operator`](https://github.com/featherbitplatform/gateway-operator): config-as-CRDs (`Route`, `Policy`, `Supernode`, `PluginConfig`, `Store`, `Consumer` + a `FeatherbitGateway` binding) validated by an admission webhook that links this crate's own validators, rendering into the chart's `config.gatewayConfigMap` for file-mode installs or the etcd prefix (`config_store::etcd::reconcile_prefix`) for etcd-mode ones. This crate is a library + thin binary since that work started (`src/lib.rs`; `tests/lib_surface.rs` pins the surface). Gateway API implementation stays planned.
```

- [ ] **Step 4: CLAUDE.md**

In "Key modules", add as the first bullet:

```markdown
- `src/lib.rs` — crate root; every module is `pub` because `featherbitplatform/gateway-operator` links this crate for its config types and validators (`tests/lib_surface.rs` pins that surface — extend it, never trim it). `src/main.rs` is only the CLI.
```

In the Helm chart bullet under "Core features", append: `` `config.gatewayConfigMap` hands `gateway.yaml` to an external ConfigMap (the operator's file-mode sink). ``

- [ ] **Step 5: Build the docs site and commit**

Run: `cd website && npm run build` (fails on broken links)
Expected: build succeeds.

```bash
git add charts/featherbit-gateway/README.md website/docs/guides/deployment.md website/docs/reference/roadmap.md CLAUDE.md
git commit -m "docs: document the library target and config.gatewayConfigMap"
```

---

### Task 6: PR and handoff to the operator plan

**Files:** none new.

- [ ] **Step 1: Full verification on the branch**

Run, from a clean tree:
```bash
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked --no-default-features --features redis-store
cargo clippy --all-targets --no-default-features --locked -- -D warnings
bash dev/chart-render-check.sh
helm lint --strict charts/featherbit-gateway
```
Expected: all clean. Paste the summary lines into the PR description.

- [ ] **Step 2: Open the PR**

```bash
git push -u origin feature/operator-lib
gh pr create --base develop --title "feat: library target, JsonSchema config types, etcd reconcile_prefix, config.gatewayConfigMap" --body "$(cat <<'EOF'
Groundwork for the Kubernetes operator (featherbitplatform/gateway-operator), per its design spec:

- `src/lib.rs` crate root, `src/main.rs` thin CLI; `tests/lib_surface.rs` pins the public surface the operator links.
- `JsonSchema` on the gateway.yaml resource types (CRD schema generation).
- `config_store::etcd::reconcile_prefix`: the store's put/delete sequence without a `SharedState` (operator's etcd sink). `commit` and the seeder reuse the same code.
- Helm: `config.gatewayConfigMap` mounts `gateway.yaml` from an external, optional ConfigMap (operator's file-mode sink). Default render is byte-identical.

Runtime behavior is unchanged.
EOF
)"
```

- [ ] **Step 3: Record the commit for the operator plan**

After merge into `develop`: `git -C gateway rev-parse develop` and paste the SHA into the operator plan's Task 1 (`featherbit = { git = "...", rev = "<sha>" }`). At the next gateway release, the operator switches to `tag = "vX.Y.Z"`.
