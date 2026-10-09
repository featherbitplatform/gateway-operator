//! Spec §5 as one pure function: everything the reconciler decides, with no
//! IO, so the whole decision table is unit-testable.

use std::collections::BTreeMap;

use featherbit::config::GatewayConfig;
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;

use sha2::{Digest, Sha256};

use crate::crd::gateway::{validate_spec, Counts, FeatherbitGatewaySpec, SinkSpec};
use crate::crd::status::*;
use crate::reconcile::render::{render, Rendered};
use crate::reconcile::select::{admitted_namespaces, select, Candidate, Exclusion, ObjectRef};
use crate::reconcile::verdict::verdicts;

#[derive(Clone, Debug)]
pub struct GatewayId {
    pub namespace: String,
    pub name: String,
    pub generation: i64,
    pub created: Option<k8s_openapi::jiff::Timestamp>,
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
    /// [`sink_fingerprint`] of the current `spec.sink`.
    pub sink_fingerprint: String,
    /// `status.sinkFingerprint` as last stored.
    pub previous_sink_fingerprint: Option<String>,
    /// The sink no longer holds what the last write put there (ConfigMap
    /// missing or edited, or an etcd re-apply is due).
    pub sink_drifted: bool,
    /// Set when another gateway owns the same sink (see [`sink_conflict`]).
    pub sink_conflict: Option<String>,
    /// Objects this gateway last reported as programmed; they stay in the sink
    /// while the gateway is not ready, so they report `GatewayNotReady`.
    pub previously_programmed: Vec<ObjectRef>,
}

/// sha256 of the serialized `spec.sink`: changes whenever the write target does.
pub fn sink_fingerprint(sink: &SinkSpec) -> String {
    let json = serde_json::to_string(sink).expect("SinkSpec serializes");
    hex::encode(Sha256::digest(json.as_bytes()))
}

fn path_nested(a: &str, b: &str) -> bool {
    let (a, b) = (a.trim_end_matches('/'), b.trim_end_matches('/'));
    a == b || b.starts_with(&format!("{a}/")) || a.starts_with(&format!("{b}/"))
}

/// Plan ruling: two gateways must not share a sink. A conflict is the
/// same namespace and ConfigMap name, or a shared etcd endpoint with equal or
/// nested prefixes. The older gateway (creationTimestamp, then namespace/name)
/// keeps writing; the newer one gets the returned message and writes nothing.
pub fn sink_conflict(
    me: &GatewayId,
    mine: &SinkSpec,
    others: &[(GatewayId, SinkSpec)],
) -> Option<String> {
    let rank = |g: &GatewayId| (g.created, g.namespace.clone(), g.name.clone());
    for (other, theirs) in others {
        if other.namespace == me.namespace && other.name == me.name {
            continue;
        }
        if rank(other) >= rank(me) {
            continue; // I am the older one; the other gateway yields.
        }
        let clash = match (
            &mine.config_map,
            &theirs.config_map,
            &mine.etcd,
            &theirs.etcd,
        ) {
            (Some(a), Some(b), _, _) => (me.namespace == other.namespace && a.name == b.name)
                .then(|| format!("ConfigMap {}/{}", me.namespace, a.name)),
            (_, _, Some(a), Some(b)) => {
                let ep = |s: &str| s.trim_end_matches('/').to_ascii_lowercase();
                let shared = a
                    .endpoints
                    .iter()
                    .any(|x| b.endpoints.iter().any(|y| ep(x) == ep(y)));
                (shared && path_nested(&a.prefix, &b.prefix))
                    .then(|| format!("etcd prefix {} (other: {})", a.prefix, b.prefix))
            }
            _ => None,
        };
        if let Some(what) = clash {
            return Some(format!(
                "spec.sink: {what} is already written by older gateway {}/{}; each gateway needs its own sink",
                other.namespace, other.name
            ));
        }
    }
    None
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

    if let Some(e) = validate_spec(&input.spec)
        .err()
        .or_else(|| input.sink_conflict.clone())
    {
        let object_statuses = input
            .previously_programmed
            .iter()
            .map(|obj| not_ready_status(obj, &me, "gateway spec is invalid"))
            .collect();
        return Plan {
            write: None,
            rendered: None,
            config: crate::validators::empty_config(),
            gateway_conditions: vec![cond(READY, false, REASON_INVALID_SPEC, &e, gen)],
            counts: Counts::default(),
            object_statuses,
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
        excluded: (selection
            .excluded
            .iter()
            .filter(|e| e.reason != REASON_NOT_SELECTED)
            .count()
            + v.excluded.len()) as u32,
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
    let up_to_date = input.previous_hash.as_deref() == Some(rendered.hash.as_str())
        && input.previous_sink_fingerprint.as_deref() == Some(input.sink_fingerprint.as_str())
        && !input.sink_drifted;
    let write = if up_to_date {
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

fn not_ready_status(obj: &ObjectRef, me: &GatewayRef, why: &str) -> ObjectStatusUpdate {
    let g = obj.generation;
    ObjectStatusUpdate {
        obj: obj.clone(),
        conditions: vec![
            cond(ACCEPTED, true, REASON_VALID, "", g),
            cond(RESOLVED_REFS, true, REASON_RESOLVED, "", g),
            cond(PROGRAMMED, false, REASON_GATEWAY_NOT_READY, why, g),
        ],
        programmed_by: None,
        gateway: me.clone(),
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
                created: None,
            },
            spec: serde_yaml::from_str(spec_yaml).unwrap(),
            namespaces: vec![],
            candidates,
            previous_hash: previous_hash.map(String::from),
            sink_fingerprint: FP.into(),
            previous_sink_fingerprint: Some(FP.into()),
            sink_drifted: false,
            sink_conflict: None,
            previously_programmed: vec![],
        }
    }

    const FP: &str = "fp-1";

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

    fn healthy_with(f: impl FnOnce(&mut Input)) -> Plan {
        let first = plan(input(
            "sink: { configMap: { name: c } }",
            vec![c(Kind::Policy, "gw", "p", POLICY_OK)],
            None,
        ));
        let hash = first.rendered.unwrap().hash;
        let mut i = input(
            "sink: { configMap: { name: c } }",
            vec![c(Kind::Policy, "gw", "p", POLICY_OK)],
            Some(&hash),
        );
        f(&mut i);
        plan(i)
    }

    #[test]
    fn changed_sink_fingerprint_forces_a_write() {
        let p = healthy_with(|i| i.previous_sink_fingerprint = Some("old".into()));
        assert!(p.write.is_some());
        let p = healthy_with(|i| i.previous_sink_fingerprint = None);
        assert!(p.write.is_some());
    }

    #[test]
    fn drifted_sink_forces_a_write() {
        let p = healthy_with(|i| i.sink_drifted = true);
        assert!(p.write.is_some());
    }

    #[test]
    fn same_hash_same_fingerprint_not_drifted_does_not_write() {
        let p = healthy_with(|_| {});
        assert!(p.write.is_none());
    }

    fn spec_of(y: &str) -> SinkSpec {
        serde_yaml::from_str(&format!("{{ {y} }}")).unwrap()
    }

    #[test]
    fn sink_fingerprint_tracks_the_target() {
        let a = sink_fingerprint(&spec_of("configMap: { name: a }"));
        assert_eq!(a, sink_fingerprint(&spec_of("configMap: { name: a }")));
        assert_ne!(a, sink_fingerprint(&spec_of("configMap: { name: b }")));
        assert_ne!(
            sink_fingerprint(&spec_of("etcd: { endpoints: ['http://e:2379'] }")),
            sink_fingerprint(&spec_of(
                "etcd: { endpoints: ['http://e:2379'], prefix: /other }"
            ))
        );
    }

    fn gid(ns: &str, name: &str, created: Option<i64>) -> GatewayId {
        GatewayId {
            namespace: ns.into(),
            name: name.into(),
            generation: 1,
            created: created.map(|s| k8s_openapi::jiff::Timestamp::from_second(s).unwrap()),
        }
    }

    #[test]
    fn configmap_sink_conflict_older_gateway_wins() {
        let old = (gid("gw", "old", Some(1)), spec_of("configMap: { name: c }"));
        let new = (gid("gw", "new", Some(2)), spec_of("configMap: { name: c }"));
        let msg = sink_conflict(&new.0, &new.1, std::slice::from_ref(&old)).unwrap();
        assert!(
            msg.contains("gw/old") && msg.contains("ConfigMap gw/c"),
            "{msg}"
        );
        assert_eq!(
            sink_conflict(&old.0, &old.1, std::slice::from_ref(&new)),
            None
        );
        assert_eq!(
            sink_conflict(&old.0, &old.1, std::slice::from_ref(&old)),
            None
        );
    }

    #[test]
    fn configmap_sink_conflict_needs_same_namespace_and_name() {
        let other_ns = (
            gid("other", "old", Some(1)),
            spec_of("configMap: { name: c }"),
        );
        let other_name = (gid("gw", "old", Some(1)), spec_of("configMap: { name: d }"));
        let me = gid("gw", "new", Some(2));
        let mine = spec_of("configMap: { name: c }");
        assert_eq!(sink_conflict(&me, &mine, &[other_ns, other_name]), None);
    }

    #[test]
    fn etcd_sink_conflict_on_shared_endpoint_with_nested_prefixes() {
        let e = "http://e:2379";
        let old = (
            gid("a", "old", Some(1)),
            spec_of(&format!("etcd: {{ endpoints: ['{e}'], prefix: /fb }}")),
        );
        let me = gid("b", "new", Some(2));
        for (prefix, expect) in [
            ("/fb", true),
            ("/fb/b", true),
            ("/fbx", false),
            ("/other", false),
        ] {
            let mine = spec_of(&format!("etcd: {{ endpoints: ['{e}'], prefix: {prefix} }}"));
            assert_eq!(
                sink_conflict(&me, &mine, std::slice::from_ref(&old)).is_some(),
                expect,
                "{prefix}"
            );
        }
        let elsewhere = spec_of("etcd: { endpoints: ['http://z:2379'], prefix: /fb }");
        assert_eq!(
            sink_conflict(&me, &elsewhere, std::slice::from_ref(&old)),
            None
        );
        let cm = spec_of("configMap: { name: c }");
        assert_eq!(sink_conflict(&me, &cm, std::slice::from_ref(&old)), None);
    }

    #[test]
    fn conflict_tie_breaks_on_namespace_then_name() {
        let a = (gid("gw", "a", Some(1)), spec_of("configMap: { name: c }"));
        let b = (gid("gw", "b", Some(1)), spec_of("configMap: { name: c }"));
        assert!(sink_conflict(&b.0, &b.1, std::slice::from_ref(&a)).is_some());
        assert!(sink_conflict(&a.0, &a.1, std::slice::from_ref(&b)).is_none());
    }

    #[test]
    fn sink_conflict_makes_the_gateway_not_ready_and_writes_nothing() {
        let first = plan(input(
            "sink: { configMap: { name: c } }",
            vec![c(Kind::Policy, "gw", "p", POLICY_OK)],
            None,
        ));
        let programmed = first.object_statuses[0].obj.clone();
        let mut i = input(
            "sink: { configMap: { name: c } }",
            vec![c(Kind::Policy, "gw", "p", POLICY_OK)],
            None,
        );
        i.sink_conflict = Some("spec.sink: clash with gw/old".into());
        i.previously_programmed = vec![programmed];
        let p = plan(i);
        assert!(p.write.is_none() && p.rendered.is_none());
        let ready = condition(&p.gateway_conditions, READY);
        assert_eq!(
            (ready.status.as_str(), ready.reason.as_str()),
            ("False", REASON_INVALID_SPEC)
        );
        assert!(ready.message.contains("gw/old"));
        assert!(p.events.iter().any(|e| e.warning && e.obj.is_none()));
        let o = &p.object_statuses[0];
        let pr = condition(&o.conditions, PROGRAMMED);
        assert_eq!(
            (pr.status.as_str(), pr.reason.as_str()),
            ("False", REASON_GATEWAY_NOT_READY)
        );
        assert!(o.programmed_by.is_none());
    }

    #[test]
    fn invalid_spec_marks_previously_programmed_objects_gateway_not_ready() {
        let obj = ObjectRef {
            kind: Kind::Route,
            namespace: "gw".into(),
            name: "r".into(),
            uid: "u".into(),
            generation: 4,
            created: None,
        };
        let mut i = input("sink: {}", vec![], None);
        i.previously_programmed = vec![obj];
        let p = plan(i);
        assert_eq!(p.object_statuses.len(), 1);
        let pr = condition(&p.object_statuses[0].conditions, PROGRAMMED);
        assert_eq!(pr.reason, REASON_GATEWAY_NOT_READY);
        assert_eq!(pr.observed_generation, Some(4));
    }

    #[test]
    fn counts_excluded_includes_conflict_losers_but_not_unselected() {
        let p = plan(input(
            "sink: { configMap: { name: c } }",
            vec![
                c(Kind::Policy, "gw", "p", POLICY_OK),
                c(Kind::Policy, "other-ns", "q", POLICY_OK),
            ],
            None,
        ));
        assert_eq!(p.counts.excluded, 0, "unselected objects are not excluded");
        let mut i = input(
            "sink: { configMap: { name: c } }\nresources: { namespaces: { from: All } }",
            vec![
                c(Kind::Policy, "gw", "dup", POLICY_OK),
                c(Kind::Policy, "other-ns", "dup", POLICY_OK),
            ],
            None,
        );
        i.namespaces = ["gw", "other-ns"]
            .iter()
            .map(|n| {
                let mut ns = Namespace::default();
                ns.metadata.name = Some(n.to_string());
                ns
            })
            .collect();
        let p = plan(i);
        assert_eq!(p.counts.excluded, 1);
        assert_eq!(p.counts.policies, 1);
    }
}
