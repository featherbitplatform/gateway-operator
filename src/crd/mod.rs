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

/// YAML 1.1 booleans that serde_yaml (YAML 1.2) emits as plain scalars but Go's
/// YAML parser (helm, kubectl) reads as `true`/`false`.
const YAML11_BOOLS: [&str; 10] = ["y", "Y", "n", "N", "yes", "Yes", "no", "No", "on", "off"];

fn quote_yaml11_bool(token: &str) -> String {
    if YAML11_BOOLS.contains(&token) || ["YES", "NO", "ON", "OFF", "On", "Off"].contains(&token) {
        format!("\"{token}\"")
    } else {
        token.to_string()
    }
}

/// Quotes plain scalars that YAML 1.1 reads as booleans: a map key (`y:`), a
/// sequence item (`- y`) or a mapping value (`k: y`). The gateway's canvas
/// `position` schema has an `y` property, which helm rejected unquoted.
fn quote_yaml11_bools(yaml: &str) -> String {
    yaml.lines()
        .map(|line| {
            let indent = line.len() - line.trim_start().len();
            let (pad, rest) = line.split_at(indent);
            let (dash, body) = match rest.strip_prefix("- ") {
                Some(b) => ("- ", b),
                None => ("", rest),
            };
            let out = match body.split_once(':') {
                Some((key, tail)) if tail.is_empty() || tail.starts_with(' ') => {
                    let tail = match tail.strip_prefix(' ') {
                        Some(v) => format!(" {}", quote_yaml11_bool(v)),
                        None => String::new(),
                    };
                    format!("{}:{tail}", quote_yaml11_bool(key))
                }
                _ => quote_yaml11_bool(body),
            };
            format!(
                "{pad}{dash}{out}
"
            )
        })
        .collect()
}

/// Multi-document YAML, deterministic order, for `featherbit-operator crds`
/// and `charts/featherbit-operator/crds/`.
pub fn all_crds_yaml() -> String {
    all_crds()
        .iter()
        .map(|c| {
            let y = serde_yaml::to_string(c).expect("crd serializes");
            format!(
                "---
{}",
                quote_yaml11_bools(&y)
            )
        })
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
