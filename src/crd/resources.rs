//! The six resource kinds. Each `spec` is the gateway's own type minus
//! `name`; the body is kept as a JSON map and converted with [`to_config`]
//! after `metadata.name` is inserted.

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{CustomResource, CustomResourceExt};
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};

use super::schema::{gateway_type_schema, structural_schema};
use super::status::ResourceStatus;

/// Free-form spec body. The CRD schema (set in [`root_schema`]) constrains it to
/// the gateway type; `#[serde(flatten)]` keeps the YAML flat.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SpecBody {
    #[serde(flatten)]
    pub body: Map<String, Value>,
}

/// Converts a spec body into the gateway type by inserting `name`.
pub fn to_config<T: DeserializeOwned>(name: &str, spec: &SpecBody) -> Result<T, String> {
    let mut obj = spec.body.clone();
    obj.insert("name".into(), Value::String(name.to_string()));
    serde_path_to_error::deserialize(Value::Object(obj)).map_err(|e| e.to_string())
}

macro_rules! resource_kind {
    ($name:ident, $root:ident, $cfg:ty, $kind:literal, $plural:literal, $short:literal $(, $printcol:literal)+) => {
        #[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize)]
        #[kube(
            group = "featherbit.io",
            version = "v1alpha1",
            kind = $kind,
            plural = $plural,
            shortname = $short,
            category = "featherbit",
            namespaced,
            status = "ResourceStatus",
            schema = "manual"
            $(, printcolumn = $printcol)+
        )]
        #[serde(transparent)]
        pub struct $name {
            pub body: SpecBody,
        }

        // `schema = "manual"`: kube-derive requires the root type to implement
        // `JsonSchema`; ours is the gateway type's schema (minus `name`) plus status.
        impl JsonSchema for $root {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                $kind.into()
            }

            fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
                root_schema::<$cfg>()
            }
        }
    };
}

macro_rules! std_cols {
    ($name:ident, $root:ident, $cfg:ty, $kind:literal, $plural:literal, $short:literal) => {
        resource_kind!(
            $name, $root, $cfg, $kind, $plural, $short,
            r#"{"name":"Accepted","type":"string","jsonPath":".status.conditions[?(@.type==\"Accepted\")].status"}"#,
            r#"{"name":"Programmed","type":"string","jsonPath":".status.conditions[?(@.type==\"Programmed\")].status"}"#,
            r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
        );
    };
}

resource_kind!(
    RouteSpec,
    Route,
    featherbit::config::RouteConfig,
    "Route",
    "routes",
    "fbroute",
    r#"{"name":"Policy","type":"string","jsonPath":".spec.policy"}"#,
    r#"{"name":"Path","type":"string","jsonPath":".spec.match.path"}"#,
    r#"{"name":"Accepted","type":"string","jsonPath":".status.conditions[?(@.type==\"Accepted\")].status"}"#,
    r#"{"name":"Programmed","type":"string","jsonPath":".status.conditions[?(@.type==\"Programmed\")].status"}"#,
    r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
);
std_cols!(
    PolicySpec,
    Policy,
    featherbit::config::PolicyConfig,
    "Policy",
    "policies",
    "fbpolicy"
);
std_cols!(
    SupernodeSpec,
    Supernode,
    featherbit::config::SupernodeConfig,
    "Supernode",
    "supernodes",
    "fbsupernode"
);
std_cols!(
    PluginConfigSpec,
    PluginConfig,
    featherbit::config::PluginConfigDef,
    "PluginConfig",
    "pluginconfigs",
    "fbpluginconfig"
);
std_cols!(
    StoreSpec,
    Store,
    featherbit::config::StoreConfig,
    "Store",
    "stores",
    "fbstore"
);
std_cols!(
    ConsumerSpec,
    Consumer,
    featherbit::consumers::ConsumerConfig,
    "Consumer",
    "consumers",
    "fbconsumer"
);

/// The six kinds, for iteration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    Route,
    Policy,
    Supernode,
    PluginConfig,
    Store,
    Consumer,
}

impl Kind {
    pub const ALL: [Kind; 6] = [
        Kind::Route,
        Kind::Policy,
        Kind::Supernode,
        Kind::PluginConfig,
        Kind::Store,
        Kind::Consumer,
    ];

    pub fn kind_str(self) -> &'static str {
        match self {
            Kind::Route => "Route",
            Kind::Policy => "Policy",
            Kind::Supernode => "Supernode",
            Kind::PluginConfig => "PluginConfig",
            Kind::Store => "Store",
            Kind::Consumer => "Consumer",
        }
    }

    pub fn plural(self) -> &'static str {
        match self {
            Kind::Route => "routes",
            Kind::Policy => "policies",
            Kind::Supernode => "supernodes",
            Kind::PluginConfig => "pluginconfigs",
            Kind::Store => "stores",
            Kind::Consumer => "consumers",
        }
    }

    pub fn from_kind_str(s: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|k| k.kind_str() == s)
    }
}

/// Object schema of a whole resource: `spec` is the gateway type minus
/// `name`, `status` is [`ResourceStatus`].
fn root_schema<T: JsonSchema>() -> schemars::Schema {
    let root = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": gateway_type_schema::<T>(),
            "status": structural_schema::<ResourceStatus>(),
        },
        "required": ["spec"],
    });
    schemars::Schema::try_from(root).expect("object schema")
}

pub fn resource_crds() -> Vec<CustomResourceDefinition> {
    vec![
        Route::crd(),
        Policy::crd(),
        Supernode::crd(),
        PluginConfig::crd(),
        Store::crd(),
        Consumer::crd(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use featherbit::config::{PolicyConfig, RouteConfig};
    use kube::CustomResourceExt;

    #[test]
    fn to_config_inserts_the_object_name() {
        let body: SpecBody = serde_json::from_value(serde_json::json!({
            "match": {"path": "/a"}, "policy": "p"
        }))
        .unwrap();
        let r: RouteConfig = to_config("r1", &body).unwrap();
        assert_eq!(r.name, "r1");
        assert_eq!(r.policy, "p");
    }

    #[test]
    fn spec_body_serializes_flat() {
        let body: SpecBody = serde_json::from_value(serde_json::json!({
            "policy": "p", "match": {"path": "/a"}
        }))
        .unwrap();
        let route = Route::new("r1", RouteSpec { body });
        let value = serde_json::to_value(&route).unwrap();
        assert_eq!(value["spec"]["policy"], "p");
        assert!(value["spec"]["body"].is_null());
    }

    #[test]
    fn to_config_reports_type_errors() {
        let body: SpecBody =
            serde_json::from_value(serde_json::json!({"nodes": "not-a-list"})).unwrap();
        let err = to_config::<PolicyConfig>("p", &body).unwrap_err();
        assert!(err.contains("nodes"), "{err}");
    }

    #[test]
    fn crds_carry_gateway_schemas_and_status() {
        let crd = Policy::crd();
        assert_eq!(crd.spec.group, "featherbit.io");
        assert_eq!(crd.spec.names.kind, "Policy");
        assert_eq!(crd.spec.names.plural, "policies");
        assert_eq!(
            crd.spec.names.categories.as_deref(),
            Some(&["featherbit".to_string()][..])
        );
        let v = &crd.spec.versions[0];
        assert_eq!(v.name, "v1alpha1");
        assert!(v
            .subresources
            .as_ref()
            .and_then(|s| s.status.as_ref())
            .is_some());
        let schema = serde_json::to_value(
            v.schema
                .as_ref()
                .unwrap()
                .open_api_v3_schema
                .as_ref()
                .unwrap(),
        )
        .unwrap();
        assert!(
            schema["properties"]["spec"]["properties"]["nodes"].is_object(),
            "{schema}"
        );
        assert!(schema["properties"]["spec"]["properties"]["name"].is_null());
        assert!(schema["properties"]["status"]["properties"]["conditions"].is_object());
        assert!(v
            .additional_printer_columns
            .as_ref()
            .unwrap()
            .iter()
            .any(|c| c.name == "Accepted"));
    }

    #[test]
    fn all_seven_crds_render_as_yaml_documents() {
        let yaml = crate::crd::all_crds_yaml();
        assert_eq!(yaml.matches("kind: CustomResourceDefinition").count(), 7);
        for name in [
            "routes",
            "policies",
            "supernodes",
            "pluginconfigs",
            "stores",
            "consumers",
            "featherbitgateways",
        ] {
            assert!(
                yaml.contains(&format!("name: {name}.featherbit.io")),
                "{name}"
            );
        }
    }

    #[test]
    fn crd_yaml_quotes_yaml11_booleans() {
        // The canvas `position` schema has an `x`/`y` pair; unquoted `y` is a
        // boolean to Go YAML and made `helm install` reject the CRDs.
        let yaml = crate::crd::all_crds_yaml();
        assert!(
            yaml.contains(
                "
- \"y\"
"
            ) || yaml.contains(
                "- \"y\"
"
            ),
            "required y"
        );
        assert!(
            yaml.contains(
                "\"y\":
"
            ),
            "property y"
        );
        assert!(!yaml.lines().any(|l| l.trim() == "- y" || l.trim() == "y:"));
    }

    #[test]
    fn kind_enum_round_trips() {
        for k in Kind::ALL {
            assert_eq!(Kind::from_kind_str(k.kind_str()), Some(k));
        }
        assert_eq!(Kind::PluginConfig.plural(), "pluginconfigs");
    }
}
