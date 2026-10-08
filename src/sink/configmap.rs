//! Spec §7.1: the ConfigMap sink. No owner reference, by design.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use sha2::{Digest, Sha256};

use super::SinkError;
use crate::reconcile::render::Rendered;

pub const FIELD_MANAGER: &str = "featherbit-operator";
pub const KEY: &str = "gateway.yaml";
pub const HASH_ANNOTATION: &str = "featherbit.io/config-hash";

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
        HASH_ANNOTATION.to_string(),
        rendered.hash.clone(),
    )]));
    cm.data = Some(BTreeMap::from([(KEY.to_string(), rendered.yaml.clone())]));
    cm
}

/// True when the sink ConfigMap no longer holds what the last write put there:
/// it is missing, its hash annotation differs from `expected_hash`, or its
/// `gateway.yaml` was edited (the hash is sha256 of the rendered text).
pub fn is_drifted(cm: Option<&ConfigMap>, expected_hash: Option<&str>) -> bool {
    let Some(cm) = cm else { return true };
    let Some(expected) = expected_hash else {
        return false; // nothing was ever written; the hash comparison forces the first write
    };
    let annotated = cm
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(HASH_ANNOTATION));
    if annotated.map(String::as_str) != Some(expected) {
        return true;
    }
    match cm.data.as_ref().and_then(|d| d.get(KEY)) {
        Some(yaml) => hex::encode(Sha256::digest(yaml.as_bytes())) != expected,
        None => true,
    }
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

    fn rendered_cm() -> (ConfigMap, String) {
        let r = crate::reconcile::render::render(&crate::validators::empty_config());
        (desired_configmap("ns", "c", "g", &r), r.hash)
    }

    #[test]
    fn missing_configmap_is_drifted() {
        assert!(is_drifted(None, Some("h")));
        assert!(is_drifted(None, None));
    }

    #[test]
    fn untouched_configmap_is_not_drifted() {
        let (cm, hash) = rendered_cm();
        assert!(!is_drifted(Some(&cm), Some(&hash)));
    }

    #[test]
    fn annotation_or_data_mismatch_is_drifted() {
        let (mut cm, hash) = rendered_cm();
        assert!(is_drifted(Some(&cm), Some("other")));
        cm.data.as_mut().unwrap().insert(
            KEY.into(),
            "routes: []
"
            .into(),
        );
        assert!(is_drifted(Some(&cm), Some(&hash)), "edited data");
        cm.data = None;
        assert!(is_drifted(Some(&cm), Some(&hash)), "key removed");
        let (mut cm, hash) = rendered_cm();
        cm.metadata.annotations = None;
        assert!(is_drifted(Some(&cm), Some(&hash)), "annotation removed");
    }
}
