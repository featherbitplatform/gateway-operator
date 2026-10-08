//! Per-object checks (spec §4.1, §6). One function per kind, used verbatim
//! by the admission webhook and the reconciler so the two never disagree.
//! Every check delegates to the gateway crate; nothing here re-derives a rule.

use featherbit::config::{
    GatewayConfig, PluginConfigDef, PolicyConfig, RouteConfig, StoreConfig, SupernodeConfig,
};
use featherbit::consumers::{ConsumerConfig, ConsumerStore};

/// Supernode boundary pseudo-types (part of the gateway's documented supernode
/// contract; the gateway keeps its own list private).
const BOUNDARY_TYPES: [&str; 3] = ["input", "output", "error"];

pub fn empty_config() -> GatewayConfig {
    serde_yaml::from_str("{}").expect("empty config parses")
}

fn known_node_type(t: &str) -> bool {
    t == "supernode" || featherbit::plugins::port_spec(t).is_some()
}

fn check_node_types(
    ctx: &str,
    nodes: &[featherbit::config::NodeConfig],
    skip_boundaries: bool,
) -> Result<(), String> {
    for n in nodes {
        if skip_boundaries && BOUNDARY_TYPES.contains(&n.node_type.as_str()) {
            continue;
        }
        if !known_node_type(&n.node_type) {
            return Err(format!(
                "{ctx}: node '{}' has unknown type '{}'",
                n.id, n.node_type
            ));
        }
    }
    Ok(())
}

pub fn check_route(r: &RouteConfig) -> Result<(), String> {
    if r.policy.trim().is_empty() {
        return Err(format!("route '{}': policy must not be empty", r.name));
    }
    featherbit::routing::validate_match_rule(&r.match_rule)
        .map_err(|e| format!("route '{}': {e}", r.name))
}

/// True when the policy references no shared object and can therefore be
/// compiled on its own. A node is NOT self-contained when it has a
/// `config_ref`, is a `type: supernode` instance, or names a Store in its
/// config: a top-level `store` key (e.g. `limit-count {policy: redis, store}`)
/// or a nested `session.store` (session plugins). Reads are shallow.
pub fn is_self_contained(p: &PolicyConfig) -> bool {
    p.nodes.iter().all(|n| {
        n.config_ref.is_none()
            && n.node_type != "supernode"
            && !n.config.contains_key("store")
            && n.config
                .get("session")
                .and_then(|s| s.get("store"))
                .is_none()
    })
}

/// Structural checks only: the gateway's graph validation and node types.
/// The reconciler uses this per object; stores and other shared objects are
/// resolved later by `compile_policy_against`.
pub fn check_policy_structure(p: &PolicyConfig) -> Result<(), String> {
    featherbit::graph::validate_policy(p)
        .map_err(|errs| format!("policy '{}': {}", p.name, errs.join("; ")))?;
    check_node_types(&format!("policy '{}'", p.name), &p.nodes, false)
}

/// Admission check: structure plus a standalone compile when the policy is
/// self-contained.
pub fn check_policy(p: &PolicyConfig) -> Result<(), String> {
    check_policy_structure(p)?;
    if is_self_contained(p) {
        compile_policy_against(p, &empty_config())?;
    }
    Ok(())
}

/// Compiles one policy against the shared objects of a selection (spec §5
/// step 5). Catches bad plugin config, unresolved `config_ref`/supernode
/// names, unwired ports after expansion and unknown `store:` names, with the
/// gateway's own messages. Uses the offline variant: the operator pod has none
/// of the gateway's files (Lua scripts, credential files, CA certs).
pub fn compile_policy_against(p: &PolicyConfig, shared: &GatewayConfig) -> Result<(), String> {
    let mut gw = empty_config();
    gw.policies.push(p.clone());
    gw.supernodes = shared.supernodes.clone();
    gw.plugin_configs = shared.plugin_configs.clone();
    gw.stores = shared.stores.clone();
    gw.consumers = shared.consumers.clone();
    featherbit::state::validate_gateway_config_offline(&gw)
}

pub fn check_supernode(s: &SupernodeConfig) -> Result<(), String> {
    featherbit::graph::validate_supernode(s)
        .map_err(|errs| format!("supernode '{}': {}", s.name, errs.join("; ")))?;
    if !s.nodes.iter().any(|n| n.node_type == "input") {
        return Err(format!(
            "supernode '{}': has no input boundary node",
            s.name
        ));
    }
    check_node_types(&format!("supernode '{}'", s.name), &s.nodes, true)
}

pub fn check_plugin_config(pc: &PluginConfigDef) -> Result<(), String> {
    if featherbit::plugins::port_spec(&pc.plugin_type).is_none() {
        return Err(format!(
            "plugin config '{}': unknown type '{}'",
            pc.name, pc.plugin_type
        ));
    }
    Ok(())
}

pub fn check_store(s: &StoreConfig) -> Result<(), String> {
    let prefixed = |e: String| {
        let prefix = format!("store '{}'", s.name);
        if e.contains(&prefix) {
            e
        } else {
            format!("{prefix}: {e}")
        }
    };
    featherbit::stores::validate_stores(std::slice::from_ref(s)).map_err(prefixed)?;
    // `validate_stores` does not parse URLs; the offline whole-config compile does.
    let mut gw = empty_config();
    gw.stores.push(s.clone());
    featherbit::state::validate_gateway_config_offline(&gw).map_err(prefixed)
}

pub fn check_consumer(c: &ConsumerConfig) -> Result<(), String> {
    ConsumerStore::from_config(std::slice::from_ref(c))
        .map(|_| ())
        .map_err(|e| format!("consumer '{}': {e}", c.name))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn policy(yaml: &str) -> PolicyConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    const OK: &str = r#"
name: ok
nodes:
  - { id: listener, type: listener }
  - { id: mock, type: mocking, config: { response_example: "{}" } }
  - { id: client, type: client }
edges:
  - { from: listener.out, to: mock.in }
  - { from: mock.success, to: client.in }
"#;

    #[test]
    fn check_policy_accepts_a_valid_self_contained_policy() {
        check_policy(&policy(OK)).unwrap();
        assert!(is_self_contained(&policy(OK)));
    }

    #[test]
    fn check_policy_rejects_unknown_node_type() {
        let err =
            check_policy(&policy(&OK.replace("type: mocking", "type: mocking-typo"))).unwrap_err();
        assert!(
            err.contains("mock") && err.contains("mocking-typo"),
            "{err}"
        );
    }

    #[test]
    fn check_policy_rejects_unwired_outcome_port_via_standalone_compile() {
        // `cors` exits preflight requests on a `preflight` port that must be wired.
        let p = policy(
            r#"
name: cors
nodes:
  - { id: listener, type: listener }
  - { id: cors, type: cors, config: { allow_origins: "*" } }
  - { id: mock, type: mocking, config: { response_example: "{}" } }
  - { id: client, type: client }
edges:
  - { from: listener.out, to: cors.in }
  - { from: cors.success, to: mock.in }
  - { from: mock.success, to: client.in }
"#,
        );
        let err = check_policy(&p).unwrap_err();
        assert!(err.contains("preflight"), "{err}");
    }

    #[test]
    fn policies_with_refs_are_not_self_contained_and_skip_the_compile() {
        let with_ref = policy(&OK.replace(
            "config: { response_example: \"{}\" }",
            "config_ref: shared-mock",
        ));
        assert!(!is_self_contained(&with_ref));
        check_policy(&with_ref).unwrap(); // structural only; the ref resolves at reconcile
        let with_sn = policy(&OK.replace(
            "type: mocking, config: { response_example: \"{}\" }",
            "type: supernode, config: { name: sn }",
        ));
        assert!(!is_self_contained(&with_sn));
        check_policy(&with_sn).unwrap();
    }

    pub(crate) const STORE_POLICY: &str = r#"
name: rl
nodes:
  - { id: listener, type: listener }
  - { id: lim, type: limit-count, config: { count: 5, time_window: 60, policy: redis, store: s1 } }
  - { id: ok, type: mocking, config: { response_example: "{}" } }
  - { id: no, type: mocking, config: { response_example: "{}" } }
  - { id: client, type: client }
edges:
  - { from: listener.out, to: lim.in }
  - { from: lim.success, to: ok.in }
  - { from: lim.limited, to: no.in }
  - { from: ok.success, to: client.in }
  - { from: no.success, to: client.in }
"#;

    #[test]
    fn policy_naming_a_store_is_not_self_contained_and_admitted_structurally() {
        let p = policy(STORE_POLICY);
        assert!(!is_self_contained(&p));
        check_policy(&p).unwrap();
        check_policy_structure(&p).unwrap();
        let sess = policy(&OK.replace(
            "config: { response_example: \"{}\" }",
            "config: { session: { store: s1 } }",
        ));
        assert!(!is_self_contained(&sess));
    }

    #[test]
    fn compile_policy_against_resolves_shared_objects() {
        let with_ref = policy(&OK.replace(
            "config: { response_example: \"{}\" }",
            "config_ref: shared-mock",
        ));
        let mut shared = empty_config();
        assert!(compile_policy_against(&with_ref, &shared)
            .unwrap_err()
            .contains("shared-mock"));
        shared.plugin_configs.push(
            serde_yaml::from_str(
                "name: shared-mock\ntype: mocking\nconfig: { response_example: '{}' }",
            )
            .unwrap(),
        );
        compile_policy_against(&with_ref, &shared).unwrap();
    }

    #[test]
    fn check_route_rejects_bad_host_and_empty_policy() {
        let r: RouteConfig =
            serde_yaml::from_str("name: r\nmatch: { path: /a, host: '*' }\npolicy: p").unwrap();
        assert!(check_route(&r).is_err());
        let r: RouteConfig =
            serde_yaml::from_str("name: r\nmatch: { path: /a }\npolicy: ''").unwrap();
        assert!(check_route(&r).unwrap_err().contains("policy"));
    }

    #[test]
    fn check_plugin_config_rejects_unknown_type() {
        let pc: PluginConfigDef = serde_yaml::from_str("name: x\ntype: nope\nconfig: {}").unwrap();
        assert!(check_plugin_config(&pc).unwrap_err().contains("nope"));
    }

    #[test]
    fn check_store_and_consumer_delegate_to_the_gateway() {
        let s: StoreConfig =
            serde_yaml::from_str("name: s\ntype: redis\nurl: 'redis://r:6379'").unwrap();
        check_store(&s).unwrap();
        let bad: StoreConfig = serde_yaml::from_str("name: s\ntype: redis\nurl: ''").unwrap();
        assert!(check_store(&bad).is_err());
        let bad: StoreConfig = serde_yaml::from_str(
            "name: s
type: redis
url: 'not a url'",
        )
        .unwrap();
        let err = check_store(&bad).unwrap_err();
        assert!(
            err.contains("store 's'") && err.contains("invalid url"),
            "{err}"
        );
        assert_eq!(err.matches("store 's'").count(), 1, "{err}");
        let c: featherbit::consumers::ConsumerConfig =
            serde_yaml::from_str("name: c\ncredentials: { key-auth: { key: k } }").unwrap();
        check_consumer(&c).unwrap();
    }

    #[test]
    fn check_supernode_requires_boundaries() {
        let sn: SupernodeConfig = serde_yaml::from_str("name: sn\nnodes: []\nedges: []").unwrap();
        assert!(check_supernode(&sn).is_err());
    }
}
