//! Custom resource definitions.
pub mod gateway; // Task 3
pub mod resources;
pub mod schema;
pub mod status;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

pub fn all_crds() -> Vec<CustomResourceDefinition> {
    let mut crds = resources::resource_crds();
    crds.push(gateway::crd());
    crds
}

/// Multi-document YAML, deterministic order, for `featherbit-operator crds`
/// and `charts/featherbit-operator/crds/`.
pub fn all_crds_yaml() -> String {
    all_crds()
        .iter()
        .map(|c| format!("---\n{}", serde_yaml::to_string(c).expect("crd serializes")))
        .collect::<String>()
}
