//! Custom resource definitions.
pub mod gateway;
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

#[cfg(test)]
mod tests {
    /// The chart ships with the operator: its version and appVersion must
    /// track the crate version, bumped in the same release commit.
    #[test]
    fn helm_chart_version_tracks_the_crate_version() {
        let chart = include_str!("../../charts/featherbit-operator/Chart.yaml");
        let v = env!("CARGO_PKG_VERSION");
        assert!(
            chart.lines().any(|l| l == format!("version: {v}")),
            "Chart.yaml `version:` must be {v}"
        );
        assert!(
            chart.lines().any(|l| l == format!("appVersion: \"{v}\"")),
            "Chart.yaml `appVersion:` must be \"{v}\""
        );
    }
}
