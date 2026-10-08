//! JSON schemas for the six resource kinds, generated from the gateway's own
//! serde types so a CRD accepts exactly what gateway.yaml accepts (minus
//! `name`, which is the object's metadata.name).

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::JSONSchemaProps;
use schemars::JsonSchema;
use serde_json::Value;

pub(crate) fn structural_schema<T: JsonSchema>() -> Value {
    let settings = schemars::generate::SchemaSettings::openapi3()
        .with(|s| {
            s.inline_subschemas = true;
            s.meta_schema = None;
        })
        .with_transform(schemars::transform::AddNullable::default())
        .with_transform(kube::core::schema::StructuralSchemaRewriter)
        .with_transform(kube::core::schema::OptionalEnum)
        .with_transform(kube::core::schema::OptionalIntOrString);
    let schema = settings.into_generator().into_root_schema_for::<T>();
    serde_json::to_value(schema).expect("schema serializes")
}

/// Structural (inlined, Kubernetes-rewritten) schema of a gateway config type
/// with the `name` property removed. Mirrors the settings `kube-derive` uses
/// for `#[derive(CustomResource)]`.
pub fn gateway_type_schema<T: JsonSchema>() -> Value {
    let mut v = structural_schema::<T>();
    if let Some(props) = v.get_mut("properties").and_then(Value::as_object_mut) {
        props.remove("name");
    }
    if let Some(req) = v.get_mut("required").and_then(Value::as_array_mut) {
        req.retain(|r| r != "name");
        if req.is_empty() {
            v.as_object_mut().unwrap().remove("required");
        }
    }
    v
}

/// The same schema as `JSONSchemaProps`, for embedding into a CRD.
pub fn spec_schema_props<T: JsonSchema>() -> JSONSchemaProps {
    serde_json::from_value(gateway_type_schema::<T>())
        .expect("structural schema is valid JSONSchemaProps")
}

/// Any serializable schema (status types) as `JSONSchemaProps`.
pub fn schema_props_of<T: JsonSchema>() -> JSONSchemaProps {
    serde_json::from_value(structural_schema::<T>()).expect("valid JSONSchemaProps")
}

#[cfg(test)]
mod tests {
    use super::*;
    use featherbit::config::{NodeConfig, PolicyConfig, StoreConfig};

    #[test]
    fn gateway_schema_drops_name_and_keeps_fields() {
        let s = gateway_type_schema::<PolicyConfig>();
        assert_eq!(s["type"], "object");
        assert!(s["properties"]["name"].is_null(), "{s}");
        assert!(s["properties"]["nodes"].is_object());
        assert!(s["properties"]["edges"].is_object());
        let required = s["required"].as_array().cloned().unwrap_or_default();
        assert!(!required.iter().any(|v| v == "name"));
        assert!(
            s.get("$ref").is_none() && s.get("$defs").is_none(),
            "must be inlined: {s}"
        );
    }

    #[test]
    fn opaque_plugin_config_preserves_unknown_fields() {
        let s = gateway_type_schema::<NodeConfig>();
        let config = &s["properties"]["config"];
        assert_eq!(config["type"], "object");
        let preserves = config["x-kubernetes-preserve-unknown-fields"] == true
            || config["additionalProperties"].is_object()
            || config["additionalProperties"] == true;
        assert!(preserves, "{config}");
    }

    #[test]
    fn store_defaults_survive_schema_generation() {
        let s = gateway_type_schema::<StoreConfig>();
        assert!(s["properties"]["key_prefix"].is_object());
        assert!(s["properties"]["url"].is_object());
    }
}
