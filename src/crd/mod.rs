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

/// YAML 1.1 booleans that the YAML 1.2 emitter writes as plain scalars but Go's
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
///
/// Also blanks whitespace-only lines: the emitter indents the empty lines
/// inside literal block scalars, which YAML reads as empty either way but
/// yamllint's trailing-spaces rule rejects.
fn quote_yaml11_bools(yaml: &str) -> String {
    yaml.lines()
        .map(|line| {
            if line.trim().is_empty() {
                return String::from("\n");
            }
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
            let y = featherbit::yaml::to_string(c).expect("crd serializes");
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
    use super::*;

    /// yamllint (`ct lint`, trailing-spaces rule) rejects the chart when the
    /// generated CRD file has trailing whitespace. Block scalars with empty
    /// lines are the usual source: an indentation-only line inside `|-` is
    /// an empty line to YAML, so blanking it changes nothing.
    #[test]
    fn crd_yaml_has_no_trailing_whitespace() {
        let yaml = all_crds_yaml();
        let offenders: Vec<&str> = yaml
            .lines()
            .filter(|l| l.ends_with(' ') || l.ends_with('\t'))
            .collect();
        assert!(offenders.is_empty(), "trailing whitespace on {offenders:?}");
        assert!(yaml.ends_with('\n') && !yaml.ends_with("\n\n"));
    }

    /// The YAML text the chart ships must describe exactly the CRDs the
    /// binary registers, whatever the emitter does to quoting or folding.
    #[test]
    fn crd_yaml_parses_back_to_the_same_definitions() {
        let yaml = all_crds_yaml();
        let parsed: Vec<CustomResourceDefinition> = yaml
            .split("\n---\n")
            .map(|doc| doc.trim_start_matches("---\n"))
            .filter(|doc| !doc.trim().is_empty())
            .map(|doc| featherbit::yaml::from_str(doc).expect("CRD document parses"))
            .collect();
        assert_eq!(parsed, all_crds());
    }

    /// Go's YAML parser (helm, kubectl) reads a bare `y` as `true`; the canvas
    /// `position` schema has a `y` property, so it must come out quoted.
    #[test]
    fn yaml11_boolean_tokens_are_quoted() {
        let yaml = all_crds_yaml();
        assert!(yaml.contains("\"y\":"), "position.y key must be quoted");
        for line in yaml.lines() {
            let body = line.trim_start().trim_start_matches("- ");
            let token = body.split_once(':').map_or(body, |(k, _)| k);
            assert!(
                !YAML11_BOOLS.contains(&token),
                "unquoted YAML 1.1 boolean token in {line:?}"
            );
        }
    }

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
