//! Spec §5 steps 1-2: which objects a gateway admits, and same-name conflicts.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, Time};
use kube::ResourceExt;

use crate::crd::gateway::{FeatherbitGatewaySpec, NamespacesFrom};
use crate::crd::resources::{Kind, SpecBody};
use crate::crd::status::{ACCEPTED, PROGRAMMED, REASON_CONFLICTED, REASON_NOT_SELECTED};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRef {
    pub kind: Kind,
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub generation: i64,
    pub created: Option<Time>,
}

impl ObjectRef {
    pub fn display(&self) -> String {
        format!("{} {}/{}", self.kind.kind_str(), self.namespace, self.name)
    }
}

impl PartialOrd for ObjectRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ObjectRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.kind, &self.namespace, &self.name).cmp(&(other.kind, &other.namespace, &other.name))
    }
}

#[derive(Clone, Debug)]
pub struct Candidate {
    pub obj: ObjectRef,
    pub spec: SpecBody,
    pub labels: BTreeMap<String, String>,
}

/// Why an object is not in a gateway's rendered config. `condition` is the
/// condition type that goes False, `reason` its reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Exclusion {
    pub obj: ObjectRef,
    pub condition: &'static str,
    pub reason: &'static str,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct Selection {
    pub kept: Vec<Candidate>,
    pub excluded: Vec<Exclusion>,
}

/// Builds a candidate from any of the six kube objects.
pub fn candidate<K>(kind: Kind, obj: &K, spec: &SpecBody) -> Candidate
where
    K: kube::Resource + ResourceExt,
{
    Candidate {
        obj: ObjectRef {
            kind,
            namespace: obj.namespace().unwrap_or_default(),
            name: obj.name_any(),
            uid: obj.uid().unwrap_or_default(),
            generation: obj.meta().generation.unwrap_or(0),
            created: obj.meta().creation_timestamp.clone(),
        },
        spec: spec.clone(),
        labels: obj
            .labels()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

pub fn admitted_namespaces(
    gateway_ns: &str,
    spec: &FeatherbitGatewaySpec,
    namespaces: &[Namespace],
) -> BTreeSet<String> {
    match spec.resources.namespaces.from {
        NamespacesFrom::Same => [gateway_ns.to_string()].into(),
        NamespacesFrom::All => namespaces
            .iter()
            .filter_map(|n| n.metadata.name.clone())
            .collect(),
        NamespacesFrom::Selector => {
            let Some(sel) = &spec.resources.namespaces.selector else {
                return BTreeSet::new();
            };
            namespaces
                .iter()
                .filter(|n| {
                    let labels = n
                        .metadata
                        .labels
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .collect();
                    label_selector_matches(sel, &labels)
                })
                .filter_map(|n| n.metadata.name.clone())
                .collect()
        }
    }
}

/// Kubernetes label-selector semantics: `matchLabels` ANDed with every
/// `matchExpressions` requirement (`In`, `NotIn`, `Exists`, `DoesNotExist`).
pub fn label_selector_matches(sel: &LabelSelector, labels: &BTreeMap<String, String>) -> bool {
    if let Some(ml) = &sel.match_labels {
        if !ml.iter().all(|(k, v)| labels.get(k) == Some(v)) {
            return false;
        }
    }
    for req in sel.match_expressions.iter().flatten() {
        let current = labels.get(&req.key);
        let values = req.values.clone().unwrap_or_default();
        let ok = match req.operator.as_str() {
            "In" => current.map(|v| values.contains(v)).unwrap_or(false),
            "NotIn" => current.map(|v| !values.contains(v)).unwrap_or(true),
            "Exists" => current.is_some(),
            "DoesNotExist" => current.is_none(),
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

pub fn select(
    spec: &FeatherbitGatewaySpec,
    admitted: &BTreeSet<String>,
    candidates: Vec<Candidate>,
) -> Selection {
    let mut out = Selection::default();
    let mut pool: Vec<Candidate> = Vec::new();
    for c in candidates {
        let selected = admitted.contains(&c.obj.namespace)
            && spec
                .resources
                .selector
                .as_ref()
                .map(|s| label_selector_matches(s, &c.labels))
                .unwrap_or(true);
        if selected {
            pool.push(c);
        } else {
            out.excluded.push(Exclusion {
                obj: c.obj,
                condition: PROGRAMMED,
                reason: REASON_NOT_SELECTED,
                message: "not selected by this gateway".into(),
            });
        }
    }
    // Oldest wins; ties on namespace then name (spec §4.1).
    pool.sort_by(|a, b| {
        a.obj
            .created
            .as_ref()
            .map(|t| t.0)
            .cmp(&b.obj.created.as_ref().map(|t| t.0))
            .then_with(|| a.obj.cmp(&b.obj))
    });
    let mut winners: BTreeMap<(Kind, String), ObjectRef> = BTreeMap::new();
    for c in pool {
        let key = (c.obj.kind, c.obj.name.clone());
        match winners.get(&key) {
            None => {
                winners.insert(key, c.obj.clone());
                out.kept.push(c);
            }
            Some(w) => out.excluded.push(Exclusion {
                obj: c.obj.clone(),
                condition: ACCEPTED,
                reason: REASON_CONFLICTED,
                message: format!(
                    "{} {}/{} is also named '{}' and is older; it wins",
                    w.kind.kind_str(),
                    w.namespace,
                    w.name,
                    w.name
                ),
            }),
        }
    }
    out.kept.sort_by(|a, b| a.obj.cmp(&b.obj));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::gateway::*;
    use k8s_openapi::api::core::v1::Namespace;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
        LabelSelector, LabelSelectorRequirement, Time,
    };

    fn ns(name: &str, labels: &[(&str, &str)]) -> Namespace {
        let mut n = Namespace::default();
        n.metadata.name = Some(name.into());
        n.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        n
    }

    fn cand(
        kind: Kind,
        namespace: &str,
        name: &str,
        created_secs: i64,
        labels: &[(&str, &str)],
    ) -> Candidate {
        Candidate {
            obj: ObjectRef {
                kind,
                namespace: namespace.into(),
                name: name.into(),
                uid: format!("{namespace}/{name}"),
                generation: 1,
                created: Some(Time(
                    k8s_openapi::jiff::Timestamp::from_second(created_secs).unwrap(),
                )),
            },
            spec: SpecBody::default(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn spec(yaml: &str) -> FeatherbitGatewaySpec {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn same_admits_only_the_gateway_namespace() {
        let s = spec("sink: { configMap: { name: c } }");
        let all = [ns("gw", &[]), ns("shop", &[])];
        let adm = admitted_namespaces("gw", &s, &all);
        assert_eq!(adm.into_iter().collect::<Vec<_>>(), ["gw"]);
    }

    #[test]
    fn all_admits_every_namespace() {
        let s = spec("sink: { configMap: { name: c } }\nresources: { namespaces: { from: All } }");
        let all = [ns("gw", &[]), ns("shop", &[]), ns("kube-system", &[])];
        assert_eq!(admitted_namespaces("gw", &s, &all).len(), 3);
    }

    #[test]
    fn namespace_selector_change_drops_objects() {
        let s = spec("sink: { configMap: { name: c } }\nresources: { namespaces: { from: Selector, selector: { matchLabels: { team: shop } } } }");
        let before = [ns("gw", &[]), ns("shop", &[("team", "shop")])];
        let adm = admitted_namespaces("gw", &s, &before);
        assert_eq!(adm.iter().collect::<Vec<_>>(), ["shop"]);
        let after = [ns("gw", &[]), ns("shop", &[("team", "billing")])];
        assert!(admitted_namespaces("gw", &s, &after).is_empty());
        let sel = select(
            &s,
            &admitted_namespaces("gw", &s, &after),
            vec![cand(Kind::Route, "shop", "r", 1, &[])],
        );
        assert!(sel.kept.is_empty());
        assert_eq!(sel.excluded.len(), 1);
        assert_eq!(
            sel.excluded[0].reason,
            crate::crd::status::REASON_NOT_SELECTED
        );
    }

    #[test]
    fn object_label_selector_filters_candidates() {
        let s = spec("sink: { configMap: { name: c } }\nresources: { selector: { matchLabels: { gateway: edge } } }");
        let adm: BTreeSet<String> = ["gw".to_string()].into();
        let sel = select(
            &s,
            &adm,
            vec![
                cand(Kind::Route, "gw", "a", 1, &[("gateway", "edge")]),
                cand(Kind::Route, "gw", "b", 1, &[("gateway", "other")]),
                cand(Kind::Route, "gw", "c", 1, &[]),
            ],
        );
        let kept: Vec<_> = sel.kept.iter().map(|c| c.obj.name.as_str()).collect();
        assert_eq!(kept, ["a"]);
        assert_eq!(sel.excluded.len(), 2);
    }

    #[test]
    fn match_expressions_are_honored() {
        let sel = LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "tier".into(),
                operator: "In".into(),
                values: Some(vec!["edge".into(), "core".into()]),
            }]),
            ..Default::default()
        };
        assert!(label_selector_matches(
            &sel,
            &[("tier".to_string(), "core".to_string())].into()
        ));
        assert!(!label_selector_matches(
            &sel,
            &[("tier".to_string(), "db".to_string())].into()
        ));
        let not_exists = LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "legacy".into(),
                operator: "DoesNotExist".into(),
                values: None,
            }]),
            ..Default::default()
        };
        assert!(label_selector_matches(&not_exists, &BTreeMap::new()));
    }

    #[test]
    fn same_name_conflict_oldest_wins() {
        let s = spec("sink: { configMap: { name: c } }\nresources: { namespaces: { from: All } }");
        let adm: BTreeSet<String> = ["a".to_string(), "b".to_string()].into();
        let sel = select(
            &s,
            &adm,
            vec![
                cand(Kind::Policy, "b", "api", 200, &[]),
                cand(Kind::Policy, "a", "api", 100, &[]),
                cand(Kind::Route, "a", "api", 100, &[]), // different kind: no conflict
            ],
        );
        assert_eq!(sel.kept.len(), 2);
        assert!(sel
            .kept
            .iter()
            .any(|c| c.obj.kind == Kind::Policy && c.obj.namespace == "a"));
        let ex = &sel.excluded[0];
        assert_eq!(ex.obj.namespace, "b");
        assert_eq!(ex.condition, crate::crd::status::ACCEPTED);
        assert_eq!(ex.reason, crate::crd::status::REASON_CONFLICTED);
        assert!(ex.message.contains("a/api"), "{}", ex.message);
    }

    #[test]
    fn conflict_tie_breaks_on_namespace_then_name() {
        let s = spec("sink: { configMap: { name: c } }\nresources: { namespaces: { from: All } }");
        let adm: BTreeSet<String> = ["a".to_string(), "b".to_string()].into();
        let sel = select(
            &s,
            &adm,
            vec![
                cand(Kind::Store, "b", "s", 5, &[]),
                cand(Kind::Store, "a", "s", 5, &[]),
            ],
        );
        assert_eq!(sel.kept[0].obj.namespace, "a");
    }
}
