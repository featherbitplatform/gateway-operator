//! The binding kind: which gateway installation receives which resources,
//! and how (spec §4.2).

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector};
use kube::{CustomResource, CustomResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "featherbit.io",
    version = "v1alpha1",
    kind = "FeatherbitGateway",
    plural = "featherbitgateways",
    shortname = "fbgw",
    category = "featherbit",
    namespaced,
    status = "GatewayStatus",
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type==\"Ready\")].status"}"#,
    printcolumn = r#"{"name":"Routes","type":"integer","jsonPath":".status.counts.routes"}"#,
    printcolumn = r#"{"name":"Policies","type":"integer","jsonPath":".status.counts.policies"}"#,
    printcolumn = r#"{"name":"Hash","type":"string","jsonPath":".status.configHash"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct FeatherbitGatewaySpec {
    pub sink: SinkSpec,
    #[serde(default)]
    pub resources: ResourceSelection,
}

/// Exactly one of the two must be set (checked by [`validate_spec`]).
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SinkSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapSink>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etcd: Option<EtcdSink>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ConfigMapSink {
    /// ConfigMap in the gateway's namespace; the operator owns its `gateway.yaml` key.
    pub name: String,
}

/// Mirrors the gateway's `EtcdConfig` (`config.etcd` chart values).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EtcdSink {
    pub endpoints: Vec<String>,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Secret with keys `user` and `password` (same shape as the chart's `config.etcd.existingSecret`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_secret_ref: Option<SecretRef>,
}

fn default_prefix() -> String {
    "/featherbit".into()
}
fn default_timeout_ms() -> u64 {
    3000
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SecretRef {
    pub name: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSelection {
    #[serde(default)]
    pub namespaces: NamespaceSelection,
    /// Label selector on the resource objects themselves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceSelection {
    #[serde(default)]
    pub from: NamespacesFrom,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum NamespacesFrom {
    #[default]
    Same,
    All,
    Selector,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayStatus {
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_rendered_at: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
    #[serde(default)]
    pub counts: Counts,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Counts {
    pub routes: u32,
    pub policies: u32,
    pub supernodes: u32,
    pub plugin_configs: u32,
    pub stores: u32,
    pub consumers: u32,
    pub excluded: u32,
}

pub fn crd() -> CustomResourceDefinition {
    FeatherbitGateway::crd()
}

/// Structural checks the schema cannot express. Used by the webhook and at
/// the top of every reconcile (`Ready=False` / `InvalidSpec`).
pub fn validate_spec(spec: &FeatherbitGatewaySpec) -> Result<(), String> {
    match (&spec.sink.config_map, &spec.sink.etcd) {
        (Some(_), None) | (None, Some(_)) => {}
        _ => return Err("spec.sink: exactly one of configMap or etcd must be set".into()),
    }
    if let Some(cm) = &spec.sink.config_map {
        if !is_dns_label(&cm.name) {
            return Err(format!(
                "spec.sink.configMap.name '{}' must be a lowercase DNS label (a-z, 0-9, '-')",
                cm.name
            ));
        }
    }
    if let Some(etcd) = &spec.sink.etcd {
        if etcd.endpoints.is_empty() {
            return Err("spec.sink.etcd.endpoints must list at least one endpoint".into());
        }
        let trimmed = etcd.prefix.trim_end_matches('/');
        if !etcd.prefix.starts_with('/') || trimmed.is_empty() {
            return Err(format!(
                "spec.sink.etcd.prefix '{}' must start with '/' and not be the root: the operator reconciles every key under it",
                etcd.prefix
            ));
        }
    }
    if spec.resources.namespaces.from == NamespacesFrom::Selector
        && spec.resources.namespaces.selector.is_none()
    {
        return Err("spec.resources.namespaces.selector is required when from is Selector".into());
    }
    if let Some(sel) = &spec.resources.selector {
        validate_label_selector("spec.resources.selector", sel)?;
    }
    if let Some(sel) = &spec.resources.namespaces.selector {
        validate_label_selector("spec.resources.namespaces.selector", sel)?;
    }
    Ok(())
}

/// Kubernetes label-selector rules. A selector the matcher cannot interpret
/// would silently select nothing and empty the sink, so reject it up front.
fn validate_label_selector(path: &str, sel: &LabelSelector) -> Result<(), String> {
    for (k, v) in sel.match_labels.iter().flatten() {
        let p = format!("{path}.matchLabels[{k}]");
        validate_label_key(&p, k)?;
        validate_label_value(&p, v)?;
    }
    for (i, req) in sel.match_expressions.iter().flatten().enumerate() {
        let p = format!("{path}.matchExpressions[{i}]");
        validate_label_key(&format!("{p}.key"), &req.key)?;
        let values = req.values.as_deref().unwrap_or(&[]);
        match req.operator.as_str() {
            "In" | "NotIn" => {
                if values.is_empty() {
                    return Err(format!(
                        "{p}.values must be non-empty for operator {}",
                        req.operator
                    ));
                }
            }
            "Exists" | "DoesNotExist" => {
                if !values.is_empty() {
                    return Err(format!(
                        "{p}.values must be empty for operator {}",
                        req.operator
                    ));
                }
            }
            other => {
                return Err(format!(
                    "{p}.operator '{other}' must be one of In, NotIn, Exists, DoesNotExist"
                ))
            }
        }
        for v in values {
            validate_label_value(&format!("{p}.values"), v)?;
        }
    }
    Ok(())
}

fn is_label_name(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && b[0].is_ascii_alphanumeric()
        && b[b.len() - 1].is_ascii_alphanumeric()
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
}

fn is_dns_subdomain(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 253
        && s.split('.').all(|part| {
            !part.is_empty()
                && part.len() <= 63
                && part
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && !part.starts_with('-')
                && !part.ends_with('-')
        })
}

fn validate_label_key(path: &str, key: &str) -> Result<(), String> {
    let ok = match key.split_once('/') {
        Some((prefix, name)) => is_dns_subdomain(prefix) && is_label_name(name),
        None => is_label_name(key),
    };
    if ok {
        Ok(())
    } else {
        Err(format!("{path}: '{key}' is not a valid label key"))
    }
}

fn validate_label_value(path: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || is_label_name(value) {
        Ok(())
    } else {
        Err(format!("{path}: '{value}' is not a valid label value"))
    }
}

fn is_dns_label(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-')
        && !s.ends_with('-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(yaml: &str) -> FeatherbitGatewaySpec {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn configmap_sink_with_defaults_is_valid() {
        let s = spec("sink: { configMap: { name: edge-config } }");
        validate_spec(&s).unwrap();
        assert_eq!(s.resources.namespaces.from, NamespacesFrom::Same);
        assert!(s.resources.selector.is_none());
    }

    #[test]
    fn etcd_sink_applies_gateway_defaults() {
        let s = spec("sink: { etcd: { endpoints: ['http://etcd:2379'] } }");
        validate_spec(&s).unwrap();
        let etcd = s.sink.etcd.unwrap();
        assert_eq!(etcd.prefix, "/featherbit");
        assert_eq!(etcd.timeout_ms, 3000);
        assert!(etcd.credentials_secret_ref.is_none());
    }

    #[test]
    fn exactly_one_sink_is_required() {
        assert!(validate_spec(&spec("sink: {}"))
            .unwrap_err()
            .contains("exactly one"));
        let both = "sink: { configMap: { name: a }, etcd: { endpoints: ['http://e:2379'] } }";
        assert!(validate_spec(&spec(both))
            .unwrap_err()
            .contains("exactly one"));
    }

    #[test]
    fn etcd_sink_needs_endpoints() {
        let err = validate_spec(&spec("sink: { etcd: { endpoints: [] } }")).unwrap_err();
        assert!(err.contains("endpoints"), "{err}");
    }

    /// A root or relative prefix would make the sink reconcile (and delete
    /// stale keys under) the whole keyspace; the gateway lib refuses an empty
    /// prefix too, but the webhook must say so at apply time.
    #[test]
    fn etcd_sink_rejects_root_or_relative_prefix() {
        for bad in ["/", "//", "featherbit", ""] {
            let err = validate_spec(&spec(&format!(
                "sink: {{ etcd: {{ endpoints: ['http://e:2379'], prefix: '{bad}' }} }}"
            )))
            .unwrap_err();
            assert!(err.contains("prefix"), "{bad}: {err}");
        }
        validate_spec(&spec(
            "sink: { etcd: { endpoints: ['http://e:2379'], prefix: /fb/edge } }",
        ))
        .unwrap();
    }

    #[test]
    fn selector_mode_needs_a_selector() {
        let s =
            spec("sink: { configMap: { name: a } }\nresources: { namespaces: { from: Selector } }");
        let err = validate_spec(&s).unwrap_err();
        assert!(err.contains("namespaces.selector"), "{err}");
        let ok = spec("sink: { configMap: { name: a } }\nresources: { namespaces: { from: Selector, selector: { matchLabels: { team: shop } } } }");
        validate_spec(&ok).unwrap();
    }

    fn with_selector(sel: &str) -> FeatherbitGatewaySpec {
        spec(&format!(
            "sink: {{ configMap: {{ name: a }} }}\nresources: {{ selector: {sel} }}"
        ))
    }

    #[test]
    fn selector_operators_are_checked() {
        let err = validate_spec(&with_selector(
            "{ matchExpressions: [{ key: tier, operator: in, values: [a] }] }",
        ))
        .unwrap_err();
        assert!(
            err.contains("spec.resources.selector.matchExpressions[0].operator"),
            "{err}"
        );
        let err = validate_spec(&with_selector(
            "{ matchExpressions: [{ key: tier, operator: Exist }] }",
        ))
        .unwrap_err();
        assert!(err.contains("operator 'Exist'"), "{err}");
    }

    #[test]
    fn selector_in_and_notin_need_values_exists_forbids_them() {
        for op in ["In", "NotIn"] {
            let err = validate_spec(&with_selector(&format!(
                "{{ matchExpressions: [{{ key: tier, operator: {op} }}] }}"
            )))
            .unwrap_err();
            assert!(err.contains("values must be non-empty"), "{err}");
        }
        for op in ["Exists", "DoesNotExist"] {
            let err = validate_spec(&with_selector(&format!(
                "{{ matchExpressions: [{{ key: tier, operator: {op}, values: [x] }}] }}"
            )))
            .unwrap_err();
            assert!(err.contains("values must be empty"), "{err}");
            validate_spec(&with_selector(&format!(
                "{{ matchExpressions: [{{ key: tier, operator: {op} }}] }}"
            )))
            .unwrap();
        }
        validate_spec(&with_selector(
            "{ matchExpressions: [{ key: tier, operator: NotIn, values: [a, \"\"] }] }",
        ))
        .unwrap();
    }

    #[test]
    fn selector_keys_and_values_follow_label_syntax() {
        let long = "a".repeat(64);
        for bad in [
            "-a",
            "a-",
            "a b",
            "Foo.com/x/y",
            "UPPER.io/x",
            "/x",
            "a/",
            &long,
        ] {
            let err = validate_spec(&with_selector(&format!(
                "{{ matchLabels: {{ \"{bad}\": v }} }}"
            )))
            .unwrap_err();
            assert!(
                err.contains("matchLabels") && err.contains("label key"),
                "{bad}: {err}"
            );
        }
        for bad in ["-v", "v-", "a b", &long] {
            let err = validate_spec(&with_selector(&format!(
                "{{ matchLabels: {{ k: \"{bad}\" }} }}"
            )))
            .unwrap_err();
            assert!(err.contains("label value"), "{bad}: {err}");
        }
        let err = validate_spec(&with_selector(
            "{ matchExpressions: [{ key: tier, operator: In, values: [\"bad value\"] }] }",
        ))
        .unwrap_err();
        assert!(err.contains("matchExpressions[0].values"), "{err}");
        validate_spec(&with_selector(
            "{ matchLabels: { \"app.kubernetes.io/name\": \"a_b.c-d\", empty: \"\" } }",
        ))
        .unwrap();
    }

    #[test]
    fn namespace_selector_is_validated_too() {
        let s = spec("sink: { configMap: { name: a } }\nresources: { namespaces: { from: Selector, selector: { matchExpressions: [{ key: team, operator: Bogus }] } } }");
        let err = validate_spec(&s).unwrap_err();
        assert!(
            err.contains("spec.resources.namespaces.selector.matchExpressions[0].operator"),
            "{err}"
        );
    }

    #[test]
    fn configmap_name_must_be_a_dns_label() {
        let err = validate_spec(&spec("sink: { configMap: { name: 'Not Valid' } }")).unwrap_err();
        assert!(err.contains("configMap.name"), "{err}");
    }

    #[test]
    fn crd_has_status_subresource_and_columns() {
        use kube::CustomResourceExt;
        let c = crd();
        assert_eq!(c.spec.names.plural, "featherbitgateways");
        let v = &c.spec.versions[0];
        assert!(v.subresources.as_ref().unwrap().status.is_some());
        let cols: Vec<_> = v
            .additional_printer_columns
            .as_ref()
            .unwrap()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(cols, ["Ready", "Routes", "Policies", "Hash", "Age"]);
        let _ = FeatherbitGateway::crd();
    }
}
