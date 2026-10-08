//! Spec §7.1: the ConfigMap sink. No owner reference, by design.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};

use super::SinkError;
use crate::reconcile::render::Rendered;

pub const FIELD_MANAGER: &str = "featherbit-operator";
pub const KEY: &str = "gateway.yaml";

pub fn desired_configmap(
    namespace: &str,
    name: &str,
    gateway: &str,
    rendered: &Rendered,
) -> ConfigMap {
    let mut cm = ConfigMap::default();
    cm.metadata.name = Some(name.into());
    cm.metadata.namespace = Some(namespace.into());
    cm.metadata.labels = Some(BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            FIELD_MANAGER.to_string(),
        ),
        ("featherbit.io/gateway".to_string(), gateway.to_string()),
    ]));
    cm.metadata.annotations = Some(BTreeMap::from([(
        "featherbit.io/config-hash".to_string(),
        rendered.hash.clone(),
    )]));
    cm.data = Some(BTreeMap::from([(KEY.to_string(), rendered.yaml.clone())]));
    cm
}

pub async fn apply(client: &Client, cm: &ConfigMap) -> Result<(), SinkError> {
    let ns = cm.metadata.namespace.clone().unwrap_or_default();
    let name = cm.metadata.name.clone().unwrap_or_default();
    Api::<ConfigMap>::namespaced(client.clone(), &ns)
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(cm),
        )
        .await
        .map(|_| ())
        .map_err(|e| SinkError(format!("applying ConfigMap {ns}/{name}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::render::Rendered;

    #[test]
    fn desired_configmap_has_key_labels_and_no_owner() {
        let r = Rendered {
            yaml: "# x\nroutes: []\n".into(),
            hash: "abc".into(),
        };
        let cm = desired_configmap("gw-ns", "edge-config", "edge", &r);
        assert_eq!(cm.metadata.name.as_deref(), Some("edge-config"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("gw-ns"));
        assert_eq!(cm.data.as_ref().unwrap()["gateway.yaml"], r.yaml);
        let labels = cm.metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels["app.kubernetes.io/managed-by"],
            "featherbit-operator"
        );
        assert_eq!(labels["featherbit.io/gateway"], "edge");
        assert_eq!(
            cm.metadata.annotations.as_ref().unwrap()["featherbit.io/config-hash"],
            "abc"
        );
        assert!(cm.metadata.owner_references.is_none());
    }
}
