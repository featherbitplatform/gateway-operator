//! Spec §6: what the webhook judges from the object alone. Pure; the HTTP
//! layer only unwraps the AdmissionReview.

use featherbit::config::{
    PluginConfigDef, PolicyConfig, RouteConfig, StoreConfig, SupernodeConfig,
};
use featherbit::consumers::ConsumerConfig;
use serde_json::Value;

use crate::crd::gateway::{validate_spec, FeatherbitGatewaySpec};
use crate::crd::resources::{to_config, Kind, SpecBody};
use crate::validators;

fn body(spec: &Value) -> Result<SpecBody, String> {
    serde_json::from_value(spec.clone()).map_err(|e| format!("spec must be an object: {e}"))
}

fn check<T: serde::de::DeserializeOwned>(
    name: &str,
    spec: &Value,
    f: impl Fn(&T) -> Result<(), String>,
) -> Result<(), String> {
    let cfg: T = to_config(name, &body(spec)?)?;
    f(&cfg)
}

/// Returns the denial message (prefixed `<Kind> <name>: `) or Ok.
pub fn admit(kind: &str, name: &str, spec: &Value) -> Result<(), String> {
    let result = match Kind::from_kind_str(kind) {
        Some(Kind::Route) => check::<RouteConfig>(name, spec, validators::check_route),
        Some(Kind::Policy) => check::<PolicyConfig>(name, spec, validators::check_policy),
        Some(Kind::Supernode) => check::<SupernodeConfig>(name, spec, validators::check_supernode),
        Some(Kind::PluginConfig) => {
            check::<PluginConfigDef>(name, spec, validators::check_plugin_config)
        }
        Some(Kind::Store) => check::<StoreConfig>(name, spec, validators::check_store),
        Some(Kind::Consumer) => check::<ConsumerConfig>(name, spec, validators::check_consumer),
        None if kind == "FeatherbitGateway" => {
            serde_json::from_value::<FeatherbitGatewaySpec>(spec.clone())
                .map_err(|e| e.to_string())
                .and_then(|s| validate_spec(&s))
        }
        None => Err(format!("unknown kind '{kind}'")),
    };
    result.map_err(|e| format!("{kind} {name}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn policy_naming_a_store_is_admitted() {
        let p: PolicyConfig = serde_yaml::from_str(crate::validators::tests::STORE_POLICY).unwrap();
        let spec = json!({ "nodes": p.nodes, "edges": p.edges });
        admit("Policy", "rl", &spec).unwrap();
    }

    #[test]
    fn workflow_policy_naming_a_store_in_a_rule_is_admitted() {
        let p: PolicyConfig =
            serde_yaml::from_str(crate::validators::tests::WORKFLOW_STORE_POLICY).unwrap();
        let spec = json!({ "nodes": p.nodes, "edges": p.edges });
        admit("Policy", "wf", &spec).unwrap();
    }

    #[test]
    fn policy_with_unknown_node_type_is_denied_with_node_and_type() {
        let err = admit("Policy", "p", &json!({
            "nodes": [{"id": "listener", "type": "listener"}, {"id": "x", "type": "nope"}, {"id": "client", "type": "client"}],
            "edges": [{"from": "listener.out", "to": "x.in"}, {"from": "x.success", "to": "client.in"}]
        })).unwrap_err();
        assert!(err.starts_with("Policy p: "), "{err}");
        assert!(err.contains("'x'") && err.contains("'nope'"), "{err}");
    }

    #[test]
    fn self_contained_policy_with_unwired_port_is_denied() {
        let err = admit("Policy", "p", &json!({
            "nodes": [{"id": "listener", "type": "listener"}, {"id": "cors", "type": "cors", "config": {"allow_origins": "*"}}, {"id": "client", "type": "client"}],
            "edges": [{"from": "listener.out", "to": "cors.in"}, {"from": "cors.success", "to": "client.in"}]
        })).unwrap_err();
        assert!(err.contains("preflight"), "{err}");
    }

    #[test]
    fn policy_with_config_ref_is_admitted_structurally() {
        admit("Policy", "p", &json!({
            "nodes": [{"id": "listener", "type": "listener"}, {"id": "m", "type": "mocking", "config_ref": "shared"}, {"id": "client", "type": "client"}],
            "edges": [{"from": "listener.out", "to": "m.in"}, {"from": "m.success", "to": "client.in"}]
        })).unwrap();
    }

    #[test]
    fn route_with_bad_host_is_denied() {
        assert!(admit(
            "Route",
            "r",
            &json!({"match": {"path": "/a", "host": "*"}, "policy": "p"})
        )
        .is_err());
        admit(
            "Route",
            "r",
            &json!({"match": {"path": "/a"}, "policy": "p"}),
        )
        .unwrap();
    }

    #[test]
    fn type_errors_are_denied_with_the_field() {
        let err = admit("Route", "r", &json!({"match": "nope", "policy": "p"})).unwrap_err();
        assert!(err.contains("match"), "{err}");
    }

    #[test]
    fn gateway_spec_is_checked() {
        assert!(admit("FeatherbitGateway", "g", &json!({"sink": {}}))
            .unwrap_err()
            .contains("exactly one"));
        admit(
            "FeatherbitGateway",
            "g",
            &json!({"sink": {"configMap": {"name": "c"}}}),
        )
        .unwrap();
    }

    #[test]
    fn unknown_kind_is_denied() {
        assert!(admit("Widget", "w", &json!({}))
            .unwrap_err()
            .contains("Widget"));
    }

    #[test]
    fn every_kind_is_routable() {
        for k in [
            "Route",
            "Policy",
            "Supernode",
            "PluginConfig",
            "Store",
            "Consumer",
            "FeatherbitGateway",
        ] {
            // Empty specs fail validation, but the kind must be recognized.
            // Some kinds (Consumer) legitimately admit an empty spec.
            let err = admit(k, "x", &json!({})).err().unwrap_or_default();
            assert!(!err.contains("unknown kind"), "{k}: {err}");
        }
    }
}
