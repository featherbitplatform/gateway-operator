//! Spec §5 steps 3-6: per-object validation, reference resolution, per-policy
//! compile against the surviving shared objects, the route cascade, and the
//! whole-config compile.

use std::collections::BTreeSet;

use featherbit::config::{
    GatewayConfig, PluginConfigDef, PolicyConfig, RouteConfig, StoreConfig, SupernodeConfig,
};
use featherbit::consumers::ConsumerConfig;

use crate::crd::resources::{to_config, Kind};
use crate::crd::status::*;
use crate::reconcile::select::{Candidate, Exclusion, ObjectRef};
use crate::validators;

#[derive(Debug)]
pub struct Verdicts {
    pub config: GatewayConfig,
    pub included: Vec<ObjectRef>,
    pub excluded: Vec<Exclusion>,
    pub whole_compile: Result<(), String>,
}

fn exclude(
    ex: &mut Vec<Exclusion>,
    obj: &ObjectRef,
    condition: &'static str,
    reason: &'static str,
    message: String,
) {
    ex.push(Exclusion {
        obj: obj.clone(),
        condition,
        reason,
        message,
    });
}

/// Parsed + per-object-checked candidates of one kind.
fn parse_kind<T: serde::de::DeserializeOwned>(
    kind: Kind,
    kept: &[Candidate],
    check: impl Fn(&T) -> Result<(), String>,
    excluded: &mut Vec<Exclusion>,
) -> Vec<(ObjectRef, T)> {
    let mut out = Vec::new();
    for c in kept.iter().filter(|c| c.obj.kind == kind) {
        match to_config::<T>(&c.obj.name, &c.spec) {
            Err(e) => exclude(
                excluded,
                &c.obj,
                ACCEPTED,
                REASON_INVALID,
                format!("spec does not parse: {e}"),
            ),
            Ok(cfg) => match check(&cfg) {
                Err(e) => exclude(excluded, &c.obj, ACCEPTED, REASON_INVALID, e),
                Ok(()) => out.push((c.obj.clone(), cfg)),
            },
        }
    }
    out
}

pub fn verdicts(kept: Vec<Candidate>) -> Verdicts {
    let mut excluded = Vec::new();

    // Step 3: per-object checks. Shared kinds first; they gate the policies.
    let stores =
        parse_kind::<StoreConfig>(Kind::Store, &kept, validators::check_store, &mut excluded);
    let consumers = parse_kind::<ConsumerConfig>(
        Kind::Consumer,
        &kept,
        validators::check_consumer,
        &mut excluded,
    );
    let plugin_configs = parse_kind::<PluginConfigDef>(
        Kind::PluginConfig,
        &kept,
        validators::check_plugin_config,
        &mut excluded,
    );
    let supernodes = parse_kind::<SupernodeConfig>(
        Kind::Supernode,
        &kept,
        validators::check_supernode,
        &mut excluded,
    );
    let policies = parse_kind::<PolicyConfig>(
        Kind::Policy,
        &kept,
        validators::check_policy_structure,
        &mut excluded,
    );
    let routes =
        parse_kind::<RouteConfig>(Kind::Route, &kept, validators::check_route, &mut excluded);

    let mut shared = validators::empty_config();
    shared.stores = stores.iter().map(|(_, s)| s.clone()).collect();
    shared.consumers = consumers.iter().map(|(_, c)| c.clone()).collect();
    shared.plugin_configs = plugin_configs.iter().map(|(_, p)| p.clone()).collect();
    shared.supernodes = supernodes.iter().map(|(_, s)| s.clone()).collect();
    let plugin_config_names: BTreeSet<&str> = shared
        .plugin_configs
        .iter()
        .map(|p| p.name.as_str())
        .collect();
    let supernode_names: BTreeSet<&str> =
        shared.supernodes.iter().map(|s| s.name.as_str()).collect();

    // Steps 4-5 for policies: references, then compile against the shared set.
    let mut good_policies: Vec<PolicyConfig> = Vec::new();
    let mut included: Vec<ObjectRef> = Vec::new();
    for (obj, p) in policies {
        let mut missing: Option<(&'static str, String)> = None;
        for n in &p.nodes {
            if let Some(r) = &n.config_ref {
                if !plugin_config_names.contains(r.as_str()) {
                    missing = Some((REASON_PLUGIN_CONFIG_NOT_FOUND, format!("node '{}' references plugin config '{r}', which is not selected or not accepted", n.id)));
                    break;
                }
            }
            if n.node_type == "supernode" {
                let name = n.config.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if !supernode_names.contains(name) {
                    missing = Some((REASON_SUPERNODE_NOT_FOUND, format!("node '{}' references supernode '{name}', which is not selected or not accepted", n.id)));
                    break;
                }
            }
        }
        if let Some((reason, msg)) = missing {
            exclude(&mut excluded, &obj, RESOLVED_REFS, reason, msg);
            continue;
        }
        if let Err(e) = validators::compile_policy_against(&p, &shared) {
            exclude(&mut excluded, &obj, ACCEPTED, REASON_COMPILE_FAILED, e);
            continue;
        }
        included.push(obj);
        good_policies.push(p);
    }
    let policy_names: BTreeSet<&str> = good_policies.iter().map(|p| p.name.as_str()).collect();
    let excluded_policy_names: BTreeSet<String> = excluded
        .iter()
        .filter(|e| e.obj.kind == Kind::Policy)
        .map(|e| e.obj.name.clone())
        .collect();

    // Step 4 for routes + cascade.
    let mut good_routes = Vec::new();
    for (obj, r) in routes {
        if policy_names.contains(r.policy.as_str()) {
            included.push(obj);
            good_routes.push(r);
        } else if excluded_policy_names.contains(&r.policy) {
            exclude(
                &mut excluded,
                &obj,
                PROGRAMMED,
                REASON_EXCLUDED,
                format!("policy '{}' is excluded", r.policy),
            );
        } else {
            exclude(
                &mut excluded,
                &obj,
                RESOLVED_REFS,
                REASON_POLICY_NOT_FOUND,
                format!("policy '{}' is not selected by this gateway", r.policy),
            );
        }
    }

    let mut config = shared;
    config.policies = good_policies;
    config.routes = good_routes;
    included.extend(stores.into_iter().map(|(o, _)| o));
    included.extend(consumers.into_iter().map(|(o, _)| o));
    included.extend(plugin_configs.into_iter().map(|(o, _)| o));
    included.extend(supernodes.into_iter().map(|(o, _)| o));
    included.sort();

    config.routes.sort_by(|a, b| a.name.cmp(&b.name));
    config.policies.sort_by(|a, b| a.name.cmp(&b.name));
    config.supernodes.sort_by(|a, b| a.name.cmp(&b.name));
    config.plugin_configs.sort_by(|a, b| a.name.cmp(&b.name));
    config.stores.sort_by(|a, b| a.name.cmp(&b.name));
    config.consumers.sort_by(|a, b| a.name.cmp(&b.name));

    // Step 6: the gateway's own whole-config gate.
    let whole_compile = featherbit::state::validate_gateway_config_offline(&config);

    Verdicts {
        config,
        included,
        excluded,
        whole_compile,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::resources::{Kind, SpecBody};
    use crate::reconcile::select::{Candidate, ObjectRef};

    fn c(kind: Kind, name: &str, yaml: &str) -> Candidate {
        Candidate {
            obj: ObjectRef {
                kind,
                namespace: "ns".into(),
                name: name.into(),
                uid: name.into(),
                generation: 1,
                created: None,
            },
            spec: SpecBody {
                body: serde_yaml::from_str(yaml).unwrap(),
            },
            labels: Default::default(),
        }
    }

    const POLICY_OK: &str = r#"
nodes:
  - { id: listener, type: listener }
  - { id: mock, type: mocking, config: { response_example: "{}" } }
  - { id: client, type: client }
edges:
  - { from: listener.out, to: mock.in }
  - { from: mock.success, to: client.in }
"#;

    fn find<'a>(v: &'a Verdicts, name: &str) -> &'a Exclusion {
        v.excluded
            .iter()
            .find(|e| e.obj.name == name)
            .unwrap_or_else(|| panic!("{name} not excluded: {:?}", v.excluded))
    }

    #[test]
    fn valid_set_is_fully_included() {
        let v = verdicts(vec![
            c(Kind::Route, "r", "match: { path: /a }\npolicy: p"),
            c(Kind::Policy, "p", POLICY_OK),
        ]);
        assert!(v.excluded.is_empty(), "{:?}", v.excluded);
        assert_eq!(v.included.len(), 2);
        assert_eq!(v.config.routes[0].name, "r");
        assert_eq!(v.config.policies[0].name, "p");
        v.whole_compile.as_ref().unwrap();
    }

    #[test]
    fn route_to_missing_policy_is_excluded_not_fatal() {
        let v = verdicts(vec![
            c(Kind::Route, "orphan", "match: { path: /a }\npolicy: nope"),
            c(Kind::Route, "ok", "match: { path: /b }\npolicy: p"),
            c(Kind::Policy, "p", POLICY_OK),
        ]);
        let e = find(&v, "orphan");
        assert_eq!(
            (e.condition, e.reason),
            (RESOLVED_REFS, REASON_POLICY_NOT_FOUND)
        );
        assert!(e.message.contains("nope"));
        assert_eq!(v.config.routes.len(), 1);
        v.whole_compile.as_ref().unwrap();
    }

    #[test]
    fn invalid_policy_cascades_to_its_routes() {
        let v = verdicts(vec![
            c(Kind::Route, "r", "match: { path: /a }\npolicy: bad"),
            c(
                Kind::Policy,
                "bad",
                "nodes: [{ id: listener, type: listener }]\nedges: []",
            ),
        ]);
        let p = find(&v, "bad");
        assert_eq!((p.condition, p.reason), (ACCEPTED, REASON_INVALID));
        let r = find(&v, "r");
        assert_eq!((r.condition, r.reason), (PROGRAMMED, REASON_EXCLUDED));
        assert!(r.message.contains("bad"));
        assert!(v.config.routes.is_empty() && v.config.policies.is_empty());
    }

    #[test]
    fn unresolved_config_ref_and_supernode_are_resolved_refs_failures() {
        let with_ref =
            POLICY_OK.replace("config: { response_example: \"{}\" }", "config_ref: shared");
        let with_sn = POLICY_OK.replace(
            "type: mocking, config: { response_example: \"{}\" }",
            "type: supernode, config: { name: sn }",
        );
        let v = verdicts(vec![
            c(Kind::Policy, "a", &with_ref),
            c(Kind::Policy, "b", &with_sn),
        ]);
        assert_eq!(find(&v, "a").reason, REASON_PLUGIN_CONFIG_NOT_FOUND);
        assert_eq!(find(&v, "b").reason, REASON_SUPERNODE_NOT_FOUND);
    }

    #[test]
    fn per_policy_compile_failure_is_compile_failed() {
        // config_ref resolves, but the shared config is bad for this node type.
        let with_ref =
            POLICY_OK.replace("config: { response_example: \"{}\" }", "config_ref: shared");
        let v = verdicts(vec![
            c(Kind::Policy, "a", &with_ref),
            c(Kind::PluginConfig, "shared", "type: mocking\nconfig: { response_example: 12345, response_status: 'not-a-number' }"),
        ]);
        let e = find(&v, "a");
        assert_eq!((e.condition, e.reason), (ACCEPTED, REASON_COMPILE_FAILED));
        assert!(
            v.included.iter().any(|o| o.name == "shared"),
            "shared config itself is fine"
        );
    }

    #[test]
    fn excluded_shared_objects_make_dependents_unresolved() {
        let with_ref =
            POLICY_OK.replace("config: { response_example: \"{}\" }", "config_ref: shared");
        let v = verdicts(vec![
            c(Kind::Policy, "a", &with_ref),
            c(
                Kind::PluginConfig,
                "shared",
                "type: no-such-type\nconfig: {}",
            ),
        ]);
        assert_eq!(find(&v, "shared").reason, REASON_INVALID);
        assert_eq!(find(&v, "a").reason, REASON_PLUGIN_CONFIG_NOT_FOUND);
    }

    #[test]
    fn undeserializable_spec_is_invalid() {
        let v = verdicts(vec![c(Kind::Route, "r", "match: 42\npolicy: p")]);
        let e = find(&v, "r");
        assert_eq!((e.condition, e.reason), (ACCEPTED, REASON_INVALID));
    }

    #[test]
    fn policy_naming_a_store_needs_the_store_selected() {
        let sp = crate::validators::tests::STORE_POLICY;
        let store = "type: redis
url: 'redis://r:6379'";
        let v = verdicts(vec![c(Kind::Policy, "rl", sp), c(Kind::Store, "s1", store)]);
        assert!(v.excluded.is_empty(), "{:?}", v.excluded);
        assert_eq!(v.config.policies.len(), 1);
        v.whole_compile.as_ref().unwrap();

        let v = verdicts(vec![c(Kind::Policy, "rl", sp)]);
        let e = find(&v, "rl");
        assert_eq!((e.condition, e.reason), (ACCEPTED, REASON_COMPILE_FAILED));
        assert!(e.message.contains("s1"), "{}", e.message);
    }

    #[test]
    fn workflow_policy_naming_a_store_needs_the_store_selected() {
        let sp = crate::validators::tests::WORKFLOW_STORE_POLICY;
        let store = "type: redis
url: 'redis://r:6379'";
        let v = verdicts(vec![c(Kind::Policy, "wf", sp), c(Kind::Store, "s1", store)]);
        assert!(v.excluded.is_empty(), "{:?}", v.excluded);
        assert_eq!(v.config.policies.len(), 1);

        let v = verdicts(vec![c(Kind::Policy, "wf", sp)]);
        let e = find(&v, "wf");
        assert_eq!((e.condition, e.reason), (ACCEPTED, REASON_COMPILE_FAILED));
        assert!(e.message.contains("s1"), "{}", e.message);
    }

    #[test]
    fn output_is_sorted_by_name() {
        let v = verdicts(vec![
            c(Kind::Policy, "zeta", POLICY_OK),
            c(Kind::Policy, "alpha", POLICY_OK),
        ]);
        let names: Vec<_> = v.config.policies.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["alpha", "zeta"]);
    }
}
