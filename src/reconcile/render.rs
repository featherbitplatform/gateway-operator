//! Spec §5 step 7: the gateway.yaml text and its hash.

use featherbit::config::GatewayConfig;
use sha2::{Digest, Sha256};

pub const HEADER: &str =
    "# Rendered by featherbit-operator; edits are overwritten on the next reconcile.\n";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rendered {
    pub yaml: String,
    pub hash: String,
}

pub fn render(config: &GatewayConfig) -> Rendered {
    let body = serde_yaml::to_string(config).expect("GatewayConfig serializes");
    let yaml = format!("{HEADER}{body}");
    let hash = hex::encode(Sha256::digest(yaml.as_bytes()));
    Rendered { yaml, hash }
}

#[cfg(test)]
mod tests {
    use super::*;
    use featherbit::config::GatewayConfig;

    fn cfg(yaml: &str) -> GatewayConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn render_is_deterministic_and_round_trips() {
        let c = cfg("routes: [{ name: r, match: { path: /a }, policy: p }]\npolicies: [{ name: p, nodes: [{ id: listener, type: listener }, { id: client, type: client }], edges: [{ from: listener.out, to: client.in }] }]");
        let a = render(&c);
        let b = render(&c);
        assert_eq!(a.yaml, b.yaml);
        assert_eq!(a.hash, b.hash);
        assert_eq!(a.hash.len(), 64);
        assert_eq!(a.hash, hex::encode(sha2::Sha256::digest(a.yaml.as_bytes())));
        assert!(a.yaml.starts_with("# Rendered by featherbit-operator"));
        let back: GatewayConfig = serde_yaml::from_str(&a.yaml).unwrap();
        assert_eq!(
            serde_json::to_value(&back).unwrap(),
            serde_json::to_value(&c).unwrap()
        );
    }

    #[test]
    fn different_configs_hash_differently() {
        let a = render(&cfg("routes: []"));
        let b = render(&cfg(
            "routes: [{ name: r, match: { path: /a }, policy: p }]",
        ));
        assert_ne!(a.hash, b.hash);
    }

    #[test]
    fn env_placeholders_pass_through_verbatim() {
        let c = cfg("stores: [{ name: s, type: redis, url: 'redis://${REDIS_HOST:-r}:6379', password: '${REDIS_PASSWORD}' }]");
        let r = render(&c);
        assert!(
            r.yaml.contains("${REDIS_HOST:-r}") && r.yaml.contains("${REDIS_PASSWORD}"),
            "{}",
            r.yaml
        );
    }

    #[test]
    fn empty_config_renders_empty_lists() {
        let r = render(&cfg("{}"));
        let back: GatewayConfig = serde_yaml::from_str(&r.yaml).unwrap();
        assert!(back.routes.is_empty() && back.policies.is_empty());
    }
}
