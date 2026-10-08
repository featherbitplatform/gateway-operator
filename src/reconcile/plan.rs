//! Spec §5 as one pure function: everything the reconciler decides, with no
//! IO, so the whole decision table is unit-testable.

use std::collections::BTreeMap;

use featherbit::config::GatewayConfig;
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;

use crate::crd::gateway::{validate_spec, Counts, FeatherbitGatewaySpec};
use crate::crd::status::*;
use crate::reconcile::render::{render, Rendered};
use crate::reconcile::select::{admitted_namespaces, select, Candidate, Exclusion, ObjectRef};
use crate::reconcile::verdict::verdicts;

#[derive(Clone, Debug)]
pub struct GatewayId {
    pub namespace: String,
    pub name: String,
    pub generation: i64,
}

impl GatewayId {
    pub fn as_ref(&self) -> GatewayRef {
        GatewayRef {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
        }
    }
}

pub struct Input {
    pub gateway: GatewayId,
    pub spec: FeatherbitGatewaySpec,
    pub namespaces: Vec<Namespace>,
    pub candidates: Vec<Candidate>,
    pub previous_hash: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ObjectStatusUpdate {
    pub obj: ObjectRef,
    pub conditions: Vec<Condition>,
    pub programmed_by: Option<GatewayRef>,
    /// The gateway that produced this verdict (even when it excluded the object).
    pub gateway: GatewayRef,
}

#[derive(Clone, Debug)]
pub struct EventSpec {
    pub obj: Option<ObjectRef>,
    pub reason: String,
    pub message: String,
    pub warning: bool,
}

pub struct Plan {
    pub write: Option<Rendered>,
    pub rendered: Option<Rendered>,
    pub config: GatewayConfig,
    pub gateway_conditions: Vec<Condition>,
    pub counts: Counts,
    pub object_statuses: Vec<ObjectStatusUpdate>,
    pub events: Vec<EventSpec>,
}

pub fn plan(input: Input) -> Plan {
    let gen = input.gateway.generation;
    let me = input.gateway.as_ref();
    let mut events = Vec::new();

    if let Err(e) = validate_spec(&input.spec) {
        return Plan {
            write: None,
            rendered: None,
            config: crate::validators::empty_config(),
            gateway_conditions: vec![cond(READY, false, REASON_INVALID_SPEC, &e, gen)],
            counts: Counts::default(),
            object_statuses: vec![],
            events: vec![EventSpec {
                obj: None,
                reason: REASON_INVALID_SPEC.into(),
                message: e,
                warning: true,
            }],
        };
    }

    let admitted = admitted_namespaces(&input.gateway.namespace, &input.spec, &input.namespaces);
    let selection = select(&input.spec, &admitted, input.candidates);
    let v = verdicts(selection.kept);

    let mut object_statuses = Vec::new();
    for obj in &v.included {
        object_statuses.push(ObjectStatusUpdate {
            obj: obj.clone(),
            conditions: vec![
                cond(ACCEPTED, true, REASON_VALID, "", obj.generation),
                cond(RESOLVED_REFS, true, REASON_RESOLVED, "", obj.generation),
                cond(
                    PROGRAMMED,
                    true,
                    REASON_PROGRAMMED,
                    &format!("rendered into {}/{}", me.namespace, me.name),
                    obj.generation,
                ),
            ],
            programmed_by: Some(me.clone()),
            gateway: me.clone(),
        });
    }
    for ex in selection.excluded.iter().chain(v.excluded.iter()) {
        object_statuses.push(status_for_exclusion(ex, &me));
        if ex.reason != REASON_NOT_SELECTED {
            events.push(EventSpec {
                obj: Some(ex.obj.clone()),
                reason: ex.reason.into(),
                message: ex.message.clone(),
                warning: true,
            });
        }
    }

    let counts = Counts {
        routes: v.config.routes.len() as u32,
        policies: v.config.policies.len() as u32,
        supernodes: v.config.supernodes.len() as u32,
        plugin_configs: v.config.plugin_configs.len() as u32,
        stores: v.config.stores.len() as u32,
        consumers: v.config.consumers.len() as u32,
        excluded: v.excluded.len() as u32,
    };

    if let Err(e) = &v.whole_compile {
        events.push(EventSpec {
            obj: None,
            reason: REASON_COMPILE_FAILED.into(),
            message: e.clone(),
            warning: true,
        });
        return Plan {
            write: None,
            rendered: None,
            config: v.config,
            gateway_conditions: vec![cond(READY, false, REASON_COMPILE_FAILED, e, gen)],
            counts,
            object_statuses: object_statuses
                .into_iter()
                .map(|mut o| {
                    if o.programmed_by.is_some() {
                        set_condition(
                            &mut o.conditions,
                            cond(
                                PROGRAMMED,
                                false,
                                REASON_GATEWAY_NOT_READY,
                                "gateway config failed to compile",
                                o.obj.generation,
                            ),
                        );
                        o.programmed_by = None;
                    }
                    o
                })
                .collect(),
            events,
        };
    }

    let rendered = render(&v.config);
    let write = if input.previous_hash.as_deref() == Some(rendered.hash.as_str()) {
        None
    } else {
        Some(rendered.clone())
    };
    Plan {
        write,
        rendered: Some(rendered),
        config: v.config,
        gateway_conditions: vec![cond(READY, true, REASON_READY, "", gen)],
        counts,
        object_statuses,
        events,
    }
}

fn status_for_exclusion(ex: &Exclusion, me: &GatewayRef) -> ObjectStatusUpdate {
    let g = ex.obj.generation;
    let conditions = match ex.condition {
        c if c == PROGRAMMED && ex.reason == REASON_NOT_SELECTED => {
            vec![cond(PROGRAMMED, false, REASON_NOT_SELECTED, &ex.message, g)]
        }
        c if c == ACCEPTED => vec![
            cond(ACCEPTED, false, ex.reason, &ex.message, g),
            cond(PROGRAMMED, false, REASON_EXCLUDED, "", g),
        ],
        c if c == RESOLVED_REFS => vec![
            cond(ACCEPTED, true, REASON_VALID, "", g),
            cond(RESOLVED_REFS, false, ex.reason, &ex.message, g),
            cond(PROGRAMMED, false, REASON_EXCLUDED, "", g),
        ],
        _ => vec![
            cond(ACCEPTED, true, REASON_VALID, "", g),
            cond(PROGRAMMED, false, ex.reason, &ex.message, g),
        ],
    };
    ObjectStatusUpdate {
        obj: ex.obj.clone(),
        conditions,
        programmed_by: None,
        gateway: me.clone(),
    }
}

#[derive(Clone, Debug)]
pub struct MergedObjectStatus {
    pub obj: ObjectRef,
    pub conditions: Vec<Condition>,
    pub gateways: Vec<GatewayRef>,
}

/// Spec §4.3: an object selected by several gateways reports the worst
/// result per condition type and names every gateway that rendered it.
/// Gateways that did not select it contribute nothing unless no gateway did.
pub fn merge_object_statuses(per_gateway: Vec<Vec<ObjectStatusUpdate>>) -> Vec<MergedObjectStatus> {
    let mut by_obj: BTreeMap<ObjectRef, Vec<ObjectStatusUpdate>> = BTreeMap::new();
    for updates in per_gateway {
        for u in updates {
            by_obj.entry(u.obj.clone()).or_default().push(u);
        }
    }
    let mut out = Vec::new();
    for (obj, updates) in by_obj {
        let selected: Vec<&ObjectStatusUpdate> = updates
            .iter()
            .filter(|u| {
                !u.conditions
                    .iter()
                    .any(|c| c.type_ == PROGRAMMED && c.reason == REASON_NOT_SELECTED)
            })
            .collect();
        let gateways: Vec<GatewayRef> = {
            let mut g: Vec<_> = selected
                .iter()
                .filter_map(|u| u.programmed_by.clone())
                .collect();
            g.sort();
            g.dedup();
            g
        };
        let mut conditions: Vec<Condition> = Vec::new();
        if selected.is_empty() {
            conditions.push(
                updates[0]
                    .conditions
                    .iter()
                    .find(|c| c.type_ == PROGRAMMED)
                    .cloned()
                    .unwrap_or_else(|| {
                        cond(PROGRAMMED, false, REASON_NOT_SELECTED, "", obj.generation)
                    }),
            );
        } else {
            for t in [ACCEPTED, RESOLVED_REFS, PROGRAMMED] {
                // Pair each update with its own condition of this type.
                let paired: Vec<(&Condition, &ObjectStatusUpdate)> = selected
                    .iter()
                    .filter_map(|u| u.conditions.iter().find(|c| c.type_ == t).map(|c| (c, *u)))
                    .collect();
                if paired.is_empty() {
                    continue;
                }
                let merged = if t == PROGRAMMED {
                    // True if any gateway rendered it.
                    paired
                        .iter()
                        .find(|(c, _)| c.status == "True")
                        .unwrap_or(&paired[0])
                        .0
                        .clone()
                } else {
                    // Worst wins: the first False from any gateway, naming it.
                    match paired.iter().find(|(c, _)| c.status == "False") {
                        Some((c, u)) => {
                            let mut c = (*c).clone();
                            c.message = format!(
                                "[{}/{}] {}",
                                u.gateway.namespace, u.gateway.name, c.message
                            );
                            c
                        }
                        None => paired[0].0.clone(),
                    }
                };
                conditions.push(merged);
            }
        }
        out.push(MergedObjectStatus {
            obj,
            conditions,
            gateways,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::resources::{Kind, SpecBody};
    use crate::reconcile::select::{Candidate, ObjectRef};

    const POLICY_OK: &str = "nodes:\n  - { id: listener, type: listener }\n  - { id: client, type: client }\nedges:\n  - { from: listener.out, to: client.in }\n";

    fn c(kind: Kind, ns: &str, name: &str, yaml: &str) -> Candidate {
        Candidate {
            obj: ObjectRef {
                kind,
                namespace: ns.into(),
                name: name.into(),
                uid: name.into(),
                generation: 3,
                created: None,
            },
            spec: SpecBody {
                body: serde_yaml::from_str(yaml).unwrap(),
            },
            labels: Default::default(),
        }
    }

    fn input(spec_yaml: &str, candidates: Vec<Candidate>, previous_hash: Option<&str>) -> Input {
        Input {
            gateway: GatewayId {
                namespace: "gw".into(),
                name: "edge".into(),
                generation: 7,
            },
            spec: serde_yaml::from_str(spec_yaml).unwrap(),
            namespaces: vec![],
            candidates,
            previous_hash: previous_hash.map(String::from),
        }
    }

    fn condition<'a>(conds: &'a [Condition], t: &str) -> &'a Condition {
        conds
            .iter()
            .find(|c| c.type_ == t)
            .unwrap_or_else(|| panic!("no {t} in {conds:?}"))
    }

    #[test]
    fn healthy_plan_writes_and_marks_ready() {
        let p = plan(input(
            "sink: { configMap: { name: c } }",
            vec![
                c(Kind::Route, "gw", "r", "match: { path: /a }\npolicy: p"),
                c(Kind::Policy, "gw", "p", POLICY_OK),
            ],
            None,
        ));
        assert!(p.write.is_some());
        let ready = condition(&p.gateway_conditions, READY);
        assert_eq!(
            (ready.status.as_str(), ready.reason.as_str()),
            ("True", REASON_READY)
        );
        assert_eq!(ready.observed_generation, Some(7));
        assert_eq!(
            p.counts,
            Counts {
                routes: 1,
                policies: 1,
                ..Default::default()
            }
        );
        let r = p
            .object_statuses
            .iter()
            .find(|o| o.obj.name == "r")
            .unwrap();
        assert_eq!(condition(&r.conditions, PROGRAMMED).status, "True");
        assert_eq!(condition(&r.conditions, ACCEPTED).status, "True");
        assert_eq!(condition(&r.conditions, RESOLVED_REFS).status, "True");
        assert_eq!(
            condition(&r.conditions, ACCEPTED).observed_generation,
            Some(3)
        );
        assert_eq!(
            r.programmed_by,
            Some(GatewayRef {
                namespace: "gw".into(),
                name: "edge".into()
            })
        );
    }

    #[test]
    fn unchanged_hash_skips_the_write() {
        let first = plan(input(
            "sink: { configMap: { name: c } }",
            vec![c(Kind::Policy, "gw", "p", POLICY_OK)],
            None,
        ));
        let hash = first.rendered.as_ref().unwrap().hash.clone();
        let second = plan(input(
            "sink: { configMap: { name: c } }",
            vec![c(Kind::Policy, "gw", "p", POLICY_OK)],
            Some(&hash),
        ));
        assert!(second.write.is_none());
        assert_eq!(condition(&second.gateway_conditions, READY).status, "True");
    }

    #[test]
    fn invalid_spec_is_not_ready_and_writes_nothing() {
        let p = plan(input("sink: {}", vec![], None));
        assert!(p.write.is_none() && p.rendered.is_none());
        let ready = condition(&p.gateway_conditions, READY);
        assert_eq!(
            (ready.status.as_str(), ready.reason.as_str()),
            ("False", REASON_INVALID_SPEC)
        );
        assert_eq!(p.events.len(), 1);
        assert_eq!(p.events[0].reason, REASON_INVALID_SPEC);
        assert!(p.events[0].obj.is_none());
    }

    #[test]
    fn excluded_objects_get_conditions_and_events_but_gateway_stays_ready() {
        let p = plan(input(
            "sink: { configMap: { name: c } }",
            vec![
                c(
                    Kind::Route,
                    "gw",
                    "orphan",
                    "match: { path: /a }\npolicy: nope",
                ),
                c(Kind::Policy, "gw", "p", POLICY_OK),
            ],
            None,
        ));
        assert_eq!(condition(&p.gateway_conditions, READY).status, "True");
        assert_eq!(p.counts.excluded, 1);
        let o = p
            .object_statuses
            .iter()
            .find(|o| o.obj.name == "orphan")
            .unwrap();
        assert_eq!(
            condition(&o.conditions, RESOLVED_REFS).reason,
            REASON_POLICY_NOT_FOUND
        );
        assert_eq!(condition(&o.conditions, PROGRAMMED).reason, REASON_EXCLUDED);
        assert_eq!(condition(&o.conditions, ACCEPTED).status, "True");
        assert!(o.programmed_by.is_none());
        assert!(p
            .events
            .iter()
            .any(|e| e.warning && e.obj.as_ref().map(|o| o.name.as_str()) == Some("orphan")));
    }

    #[test]
    fn unselected_objects_are_not_selected() {
        let p = plan(input(
            "sink: { configMap: { name: c } }",
            vec![c(Kind::Policy, "other-ns", "p", POLICY_OK)],
            None,
        ));
        let o = &p.object_statuses[0];
        assert_eq!(
            condition(&o.conditions, PROGRAMMED).reason,
            REASON_NOT_SELECTED
        );
        assert!(
            o.conditions.iter().all(|c| c.type_ != ACCEPTED),
            "no Accepted verdict without selection"
        );
        assert_eq!(p.counts.policies, 0);
    }

    #[test]
    fn plan_merges_status_across_gateways() {
        let obj = ObjectRef {
            kind: Kind::Route,
            namespace: "a".into(),
            name: "r".into(),
            uid: "u".into(),
            generation: 1,
            created: None,
        };
        let g1 = GatewayRef {
            namespace: "gw".into(),
            name: "one".into(),
        };
        let g2 = GatewayRef {
            namespace: "gw".into(),
            name: "two".into(),
        };
        let included = ObjectStatusUpdate {
            obj: obj.clone(),
            conditions: vec![
                cond(ACCEPTED, true, REASON_VALID, "", 1),
                cond(RESOLVED_REFS, true, REASON_RESOLVED, "", 1),
                cond(PROGRAMMED, true, REASON_PROGRAMMED, "", 1),
            ],
            programmed_by: Some(g1.clone()),
            gateway: g1.clone(),
        };
        let excluded = ObjectStatusUpdate {
            obj: obj.clone(),
            conditions: vec![
                cond(ACCEPTED, true, REASON_VALID, "", 1),
                cond(
                    RESOLVED_REFS,
                    false,
                    REASON_POLICY_NOT_FOUND,
                    "policy 'p' is not selected by this gateway",
                    1,
                ),
                cond(PROGRAMMED, false, REASON_EXCLUDED, "", 1),
            ],
            programmed_by: None,
            gateway: g2.clone(),
        };
        let not_selected = ObjectStatusUpdate {
            obj: obj.clone(),
            conditions: vec![cond(PROGRAMMED, false, REASON_NOT_SELECTED, "", 1)],
            programmed_by: None,
            gateway: g1.clone(),
        };
        let merged =
            merge_object_statuses(vec![vec![included], vec![excluded], vec![not_selected]]);
        assert_eq!(merged.len(), 1);
        let m = &merged[0];
        assert_eq!(m.gateways, vec![g1.clone()]);
        // Worst result wins per condition type; a gateway that merely did not select it is ignored.
        assert_eq!(condition(&m.conditions, RESOLVED_REFS).status, "False");
        assert!(condition(&m.conditions, RESOLVED_REFS)
            .message
            .contains("gw/two"));
        // Programmed is True because at least one gateway rendered it.
        assert_eq!(condition(&m.conditions, PROGRAMMED).status, "True");
    }

    #[test]
    fn object_selected_by_no_gateway_is_not_selected_after_merge() {
        let obj = ObjectRef {
            kind: Kind::Route,
            namespace: "a".into(),
            name: "r".into(),
            uid: "u".into(),
            generation: 1,
            created: None,
        };
        let g1 = GatewayRef {
            namespace: "gw".into(),
            name: "one".into(),
        };
        let ns = ObjectStatusUpdate {
            obj,
            conditions: vec![cond(PROGRAMMED, false, REASON_NOT_SELECTED, "", 1)],
            programmed_by: None,
            gateway: g1,
        };
        let merged = merge_object_statuses(vec![vec![ns.clone()], vec![ns]]);
        assert_eq!(merged[0].conditions.len(), 1);
        assert_eq!(merged[0].conditions[0].reason, REASON_NOT_SELECTED);
        assert!(merged[0].gateways.is_empty());
    }

    #[test]
    fn whole_compile_failure_is_not_ready_and_unprograms_objects() {
        let dup = "credentials: { key-auth: { key: same } }";
        let p = plan(input(
            "sink: { configMap: { name: c } }",
            vec![
                c(Kind::Consumer, "gw", "a", dup),
                c(Kind::Consumer, "gw", "b", dup),
            ],
            None,
        ));
        let ready = condition(&p.gateway_conditions, READY);
        assert_eq!(
            (ready.status.as_str(), ready.reason.as_str()),
            ("False", REASON_COMPILE_FAILED)
        );
        assert!(p.write.is_none() && p.rendered.is_none());
        assert_eq!(p.object_statuses.len(), 2);
        for o in &p.object_statuses {
            let pr = condition(&o.conditions, PROGRAMMED);
            assert_eq!(
                (pr.status.as_str(), pr.reason.as_str()),
                ("False", REASON_GATEWAY_NOT_READY)
            );
            assert!(o.programmed_by.is_none());
        }
        assert!(p
            .events
            .iter()
            .any(|e| e.reason == REASON_COMPILE_FAILED && e.warning && e.obj.is_none()));
    }

    #[test]
    fn invalid_object_gets_accepted_false_and_programmed_excluded() {
        let bad = POLICY_OK.replace("type: client", "type: no-such-node");
        let p = plan(input(
            "sink: { configMap: { name: c } }",
            vec![c(Kind::Policy, "gw", "bad", &bad)],
            None,
        ));
        let o = p
            .object_statuses
            .iter()
            .find(|o| o.obj.name == "bad")
            .unwrap();
        let a = condition(&o.conditions, ACCEPTED);
        assert_eq!(
            (a.status.as_str(), a.reason.as_str()),
            ("False", REASON_INVALID)
        );
        let pr = condition(&o.conditions, PROGRAMMED);
        assert_eq!(
            (pr.status.as_str(), pr.reason.as_str()),
            ("False", REASON_EXCLUDED)
        );
        assert!(o.conditions.iter().all(|c| c.type_ != RESOLVED_REFS));
        assert!(o.programmed_by.is_none());
        assert!(p
            .events
            .iter()
            .any(|e| e.warning && e.obj.as_ref().map(|o| o.name.as_str()) == Some("bad")));
    }
}
