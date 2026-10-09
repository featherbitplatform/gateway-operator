//! The reconcile pipeline. `select`, `verdict`, `render` and `plan` are pure
//! and unit-tested; `status`, the sinks and the controller in this file do IO.
pub mod plan;
pub mod render;
pub mod select;
pub mod status;
pub mod verdict;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::events::{Recorder, Reporter};
use kube::runtime::reflector::{self, ObjectRef as KubeObjectRef, Store};
use kube::runtime::{watcher, WatchStreamExt};
use kube::{Api, Client, Resource, ResourceExt};

use crate::crd::gateway::{validate_spec, FeatherbitGateway, GatewayStatus, SinkSpec};
use crate::crd::resources::{
    Consumer, Kind, PluginConfig, Policy, Route, Store as StoreCr, Supernode,
};
use crate::crd::status::{
    cond, set_condition, ResourceStatus, PROGRAMMED, READY, REASON_GATEWAY_NOT_READY,
    REASON_NOT_SELECTED, REASON_PROGRAMMED, REASON_SINK_UNAVAILABLE,
};
use crate::reconcile::plan::{
    merge_object_statuses, plan, EventSpec, GatewayId, Input, MergedObjectStatus,
    ObjectStatusUpdate,
};
use crate::reconcile::select::{candidate, Candidate, ObjectRef};
use crate::sink::{self, Target};
use crate::telemetry::{self, Readiness};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kube: {0}")]
    Kube(#[from] kube::Error),
    #[error("sink: {0}")]
    Sink(#[from] sink::SinkError),
}

type ObjKey = (Kind, String, String);

fn key_of(o: &ObjectRef) -> ObjKey {
    (o.kind, o.namespace.clone(), o.name.clone())
}

pub struct Context {
    pub client: Client,
    pub recorder: Recorder,
    pub gateways: Store<FeatherbitGateway>,
    pub namespaces: Store<Namespace>,
    pub routes: Store<Route>,
    pub policies: Store<Policy>,
    pub supernodes: Store<Supernode>,
    pub plugin_configs: Store<PluginConfig>,
    pub stores: Store<StoreCr>,
    pub consumers: Store<Consumer>,
    /// ConfigMaps the operator manages, for sink drift detection.
    pub configmaps: Store<ConfigMap>,
    /// When this process last applied each etcd-sink gateway (key `ns/name`);
    /// etcd is not watched, so a re-apply is due every [`REQUEUE_OK`].
    pub etcd_applied: std::sync::Mutex<HashMap<String, Instant>>,
    /// Consecutive failed reconciles per gateway (key `ns/name`), for backoff.
    pub attempts: std::sync::Mutex<HashMap<String, u32>>,
    /// Last per-gateway object statuses, for the cross-gateway merge.
    pub last_object_statuses: tokio::sync::Mutex<BTreeMap<String, Vec<ObjectStatusUpdate>>>,
    /// Objects covered by the previous merge, so objects that drop out of every
    /// gateway's view (e.g. their last gateway was deleted) get a final status.
    pub last_merged: tokio::sync::Mutex<BTreeMap<ObjKey, ObjectRef>>,
}

impl Context {
    fn etcd_reapply_due(&self, key: &str) -> bool {
        self.etcd_applied
            .lock()
            .expect("etcd_applied lock")
            .get(key)
            .map(|t| t.elapsed() >= REQUEUE_OK)
            .unwrap_or(true)
    }

    fn candidates(&self) -> Vec<Candidate> {
        let mut out = Vec::new();
        out.extend(
            self.routes
                .state()
                .iter()
                .map(|o| candidate(Kind::Route, &**o, &o.spec.body)),
        );
        out.extend(
            self.policies
                .state()
                .iter()
                .map(|o| candidate(Kind::Policy, &**o, &o.spec.body)),
        );
        out.extend(
            self.supernodes
                .state()
                .iter()
                .map(|o| candidate(Kind::Supernode, &**o, &o.spec.body)),
        );
        out.extend(
            self.plugin_configs
                .state()
                .iter()
                .map(|o| candidate(Kind::PluginConfig, &**o, &o.spec.body)),
        );
        out.extend(
            self.stores
                .state()
                .iter()
                .map(|o| candidate(Kind::Store, &**o, &o.spec.body)),
        );
        out.extend(
            self.consumers
                .state()
                .iter()
                .map(|o| candidate(Kind::Consumer, &**o, &o.spec.body)),
        );
        out
    }

    /// The object's status as currently cached: `None` when the object is no
    /// longer in the cache, `Some(None)` when it has no status yet.
    fn current_status(&self, obj: &ObjectRef) -> Option<Option<ResourceStatus>> {
        macro_rules! get {
            ($s:expr) => {
                $s.get(&KubeObjectRef::new(&obj.name).within(&obj.namespace))
                    .map(|o| o.status.clone())
            };
        }
        match obj.kind {
            Kind::Route => get!(self.routes),
            Kind::Policy => get!(self.policies),
            Kind::Supernode => get!(self.supernodes),
            Kind::PluginConfig => get!(self.plugin_configs),
            Kind::Store => get!(self.stores),
            Kind::Consumer => get!(self.consumers),
        }
    }
}

/// Applies `planned` onto `current` with Kubernetes condition semantics:
/// `lastTransitionTime` survives when the status value is unchanged, and
/// condition types that are no longer planned are dropped.
pub fn merge_conditions(current: &[Condition], planned: &[Condition]) -> Vec<Condition> {
    let mut out = current.to_vec();
    for c in planned {
        set_condition(&mut out, c.clone());
    }
    out.retain(|c| planned.iter().any(|p| p.type_ == c.type_));
    out
}

/// The status to store for an object and whether it differs from `current`.
pub fn next_status(
    current: Option<&ResourceStatus>,
    update: &MergedObjectStatus,
) -> (ResourceStatus, bool) {
    let base: &[Condition] = current.map(|s| s.conditions.as_slice()).unwrap_or(&[]);
    let next = ResourceStatus {
        conditions: merge_conditions(base, &update.conditions),
        observed_generation: Some(update.obj.generation),
        gateways: update.gateways.clone(),
    };
    let changed = current != Some(&next) && !(current.is_none() && next == Default::default());
    (next, changed)
}

pub const REQUEUE_OK: Duration = Duration::from_secs(600);

const BACKOFF_BASE: Duration = Duration::from_secs(30);
const BACKOFF_CAP: Duration = Duration::from_secs(300);

/// Delay before retry number `attempt` (0-based): 30 s doubling, capped at 300 s.
pub fn backoff_delay(attempt: u32) -> Duration {
    let factor = 1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX);
    BACKOFF_BASE.saturating_mul(factor).min(BACKOFF_CAP)
}

/// Merges every live gateway's last verdicts, patches the objects whose status
/// changed, gives objects that dropped out of every view a final
/// `Programmed=False/NotSelected`, and returns the objects patched.
async fn publish_objects(ctx: &Context) -> BTreeSet<ObjKey> {
    let mut previous = ctx.last_merged.lock().await;
    let merged = {
        let mut last = ctx.last_object_statuses.lock().await;
        let live: BTreeSet<String> = ctx
            .gateways
            .state()
            .iter()
            .map(|g| format!("{}/{}", g.namespace().unwrap_or_default(), g.name_any()))
            .collect();
        last.retain(|k, _| {
            let keep = live.contains(k);
            if !keep {
                telemetry::forget_gateway(k);
            }
            keep
        });
        merge_object_statuses(last.values().cloned().collect())
    };

    let mut changed = BTreeSet::new();
    let mut now: BTreeMap<ObjKey, ObjectRef> = BTreeMap::new();
    let mut updates: Vec<MergedObjectStatus> = merged;
    for m in &updates {
        now.insert(key_of(&m.obj), m.obj.clone());
    }
    for (k, old) in previous.iter() {
        if now.contains_key(k) {
            continue;
        }
        if let Some(Some(cur)) = ctx.current_status(old) {
            let mut conditions = cur.conditions.clone();
            set_condition(
                &mut conditions,
                cond(PROGRAMMED, false, REASON_NOT_SELECTED, "", old.generation),
            );
            updates.push(MergedObjectStatus {
                obj: old.clone(),
                conditions,
                gateways: vec![],
            });
        }
    }
    for m in &updates {
        let Some(cur) = ctx.current_status(&m.obj) else {
            continue; // deleted since the merge
        };
        let (next, differs) = next_status(cur.as_ref(), m);
        if !differs {
            continue;
        }
        match status::patch_object_status(&ctx.client, &m.obj, &next).await {
            Ok(()) => {
                changed.insert(key_of(&m.obj));
            }
            Err(e) => tracing::warn!(object = %m.obj.display(), error = %e, "status patch failed"),
        }
    }
    *previous = now;
    changed
}

pub async fn reconcile(gw: Arc<FeatherbitGateway>, ctx: Arc<Context>) -> Result<Action, Error> {
    let started = Instant::now();
    let key = format!("{}/{}", gw.namespace().unwrap_or_default(), gw.name_any());
    let r = reconcile_inner(gw, ctx.clone(), &key, started).await;
    if r.is_ok() {
        ctx.attempts.lock().expect("attempts lock").remove(&key);
    }
    if let Err(Error::Kube(_)) = &r {
        telemetry::reconcile_observed(&key, "kube_error", started.elapsed().as_secs_f64());
    }
    r
}

async fn reconcile_inner(
    gw: Arc<FeatherbitGateway>,
    ctx: Arc<Context>,
    key: &str,
    started: Instant,
) -> Result<Action, Error> {
    let ns = gw.namespace().unwrap_or_default();
    let name = gw.name_any();
    let generation = gw.metadata.generation.unwrap_or(0);
    if ctx.gateways.get(&KubeObjectRef::from_obj(&*gw)).is_none() {
        // Deleted; the deletion watcher cleans up its objects and metrics.
        return Ok(Action::await_change());
    }
    let previous_status = gw.status.clone().unwrap_or_default();
    let previous_hash = previous_status.config_hash.clone();

    let me = GatewayId {
        namespace: ns.clone(),
        name: name.clone(),
        generation,
        created: gw.metadata.creation_timestamp.as_ref().map(|t| t.0),
    };
    let sink_fingerprint = plan::sink_fingerprint(&gw.spec.sink);
    let others: Vec<(GatewayId, SinkSpec)> = ctx
        .gateways
        .state()
        .iter()
        .filter(|g| validate_spec(&g.spec).is_ok())
        .map(|g| {
            (
                GatewayId {
                    namespace: g.namespace().unwrap_or_default(),
                    name: g.name_any(),
                    generation: g.metadata.generation.unwrap_or(0),
                    created: g.metadata.creation_timestamp.as_ref().map(|t| t.0),
                },
                g.spec.sink.clone(),
            )
        })
        .collect();
    let sink_conflict = plan::sink_conflict(&me, &gw.spec.sink, &others);
    let sink_drifted = match (&gw.spec.sink.config_map, &gw.spec.sink.etcd) {
        (Some(cm), _) => {
            let found = ctx
                .configmaps
                .get(&KubeObjectRef::new(&cm.name).within(&ns));
            sink::configmap::is_drifted(found.as_deref(), previous_hash.as_deref())
        }
        (None, Some(_)) => ctx.etcd_reapply_due(key),
        _ => false,
    };
    let previously_programmed: Vec<ObjectRef> = ctx
        .last_object_statuses
        .lock()
        .await
        .get(key)
        .map(|updates| {
            updates
                .iter()
                .filter(|u| {
                    u.conditions.iter().any(|c| {
                        c.type_ == PROGRAMMED
                            && (c.reason == REASON_PROGRAMMED
                                || c.reason == REASON_GATEWAY_NOT_READY)
                    })
                })
                .map(|u| u.obj.clone())
                .collect()
        })
        .unwrap_or_default();

    let p = plan(Input {
        gateway: me,
        sink_fingerprint: sink_fingerprint.clone(),
        previous_sink_fingerprint: previous_status.sink_fingerprint.clone(),
        sink_drifted,
        sink_conflict,
        previously_programmed,
        spec: gw.spec.clone(),
        namespaces: ctx
            .namespaces
            .state()
            .iter()
            .map(|n| (**n).clone())
            .collect(),
        candidates: ctx.candidates(),
        previous_hash: previous_hash.clone(),
    });

    let gw_ref = ObjectReference {
        api_version: Some("featherbit.io/v1alpha1".into()),
        kind: Some("FeatherbitGateway".into()),
        name: Some(name.clone()),
        namespace: Some(ns.clone()),
        uid: gw.uid(),
        ..Default::default()
    };

    // Sink write (spec §5 step 8): when the hash or the sink target changed, or
    // the sink drifted (missing/edited ConfigMap, periodic etcd re-apply).
    if let Some(rendered) = &p.write {
        let target = Target::from_spec(&ns, &name, &gw.spec);
        if let Err(e) = sink::write(&ctx.client, &target, rendered, &p.config).await {
            tracing::error!(gateway = %key, error = %e, "sink write failed");
            let mut planned = p.gateway_conditions.clone();
            set_condition(
                &mut planned,
                cond(
                    READY,
                    false,
                    REASON_SINK_UNAVAILABLE,
                    &e.to_string(),
                    generation,
                ),
            );
            // Keep the previously applied hash so the next reconcile retries the write.
            let status = GatewayStatus {
                conditions: merge_conditions(&previous_status.conditions, &planned),
                observed_generation: gw.metadata.generation,
                config_hash: previous_hash,
                sink_fingerprint: previous_status.sink_fingerprint.clone(),
                last_rendered_at: previous_status.last_rendered_at.clone(),
                counts: p.counts.clone(),
            };
            if gw.status.as_ref() != Some(&status) {
                status::patch_gateway(&ctx.client, &ns, &name, &status).await?;
            }
            status::emit(
                &ctx.recorder,
                &gw_ref,
                &EventSpec {
                    obj: None,
                    reason: REASON_SINK_UNAVAILABLE.into(),
                    message: e.to_string(),
                    warning: true,
                },
            )
            .await;
            telemetry::reconcile_observed(key, "sink_error", started.elapsed().as_secs_f64());
            return Err(Error::Sink(e));
        }
        if gw.spec.sink.etcd.is_some() {
            ctx.etcd_applied
                .lock()
                .expect("etcd_applied lock")
                .insert(key.to_string(), Instant::now());
        }
    }
    let rendered_hash = p.rendered.as_ref().map(|r| r.hash.clone());
    if let Some(h) = &rendered_hash {
        telemetry::rendered_hash(key, h);
    }
    let mut result = "ok";
    if p.write.is_none()
        && p.gateway_conditions
            .iter()
            .any(|c| c.type_ == READY && c.status == "False")
    {
        result = "not_ready";
    }

    // Gateway status; patched only when it differs from what is stored. A
    // failed compile keeps the last applied hash: the sink still holds it.
    let last_rendered_at = if p.write.is_some() {
        Some(Time(k8s_openapi::jiff::Timestamp::now()))
    } else {
        previous_status.last_rendered_at.clone()
    };
    let status = GatewayStatus {
        conditions: merge_conditions(&previous_status.conditions, &p.gateway_conditions),
        observed_generation: gw.metadata.generation,
        config_hash: rendered_hash.or(previous_hash),
        // Only a rendered config proves the sink matches this spec.sink.
        sink_fingerprint: if p.rendered.is_some() {
            Some(sink_fingerprint)
        } else {
            previous_status.sink_fingerprint.clone()
        },
        last_rendered_at,
        counts: p.counts.clone(),
    };
    let gateway_changed = gw.status.as_ref() != Some(&status);
    if gateway_changed {
        status::patch_gateway(&ctx.client, &ns, &name, &status).await?;
    }

    // Object statuses: merge this gateway's verdicts with the other gateways' last ones.
    ctx.last_object_statuses
        .lock()
        .await
        .insert(key.to_string(), p.object_statuses.clone());
    let changed_objects = publish_objects(&ctx).await;

    // Events (only for what changed in this pass) and metrics.
    for e in &p.events {
        let target = match &e.obj {
            Some(o) => {
                if !changed_objects.contains(&key_of(o)) {
                    continue;
                }
                status::object_reference(o)
            }
            None => {
                if !gateway_changed {
                    continue;
                }
                gw_ref.clone()
            }
        };
        status::emit(&ctx.recorder, &target, e).await;
    }
    let mut per_reason: BTreeMap<(String, String), u64> = Default::default();
    for o in &p.object_statuses {
        for c in &o.conditions {
            if c.status == "False" && c.type_ != PROGRAMMED {
                *per_reason
                    .entry((o.obj.kind.kind_str().to_string(), c.reason.clone()))
                    .or_default() += 1;
            }
        }
    }
    telemetry::excluded_clear(key);
    for ((kind, reason), n) in per_reason {
        telemetry::excluded_set(key, &kind, &reason, n);
    }
    telemetry::reconcile_observed(key, result, started.elapsed().as_secs_f64());
    Ok(Action::requeue(REQUEUE_OK))
}

pub fn error_policy(gw: Arc<FeatherbitGateway>, err: &Error, ctx: Arc<Context>) -> Action {
    let key = format!("{}/{}", gw.namespace().unwrap_or_default(), gw.name_any());
    let attempt = {
        let mut attempts = ctx.attempts.lock().expect("attempts lock");
        let n = attempts.entry(key).or_insert(0);
        let current = *n;
        *n = n.saturating_add(1);
        current
    };
    let delay = backoff_delay(attempt);
    tracing::warn!(error = %err, attempt, delay_secs = delay.as_secs(), "reconcile failed; backing off");
    Action::requeue(delay)
}

/// Spawns a reflector for `T` and returns its `Store` plus a trigger stream.
///
/// The trigger yields only after the store has applied the event, so a
/// reconcile it causes never reads a cache older than the change. Triggers
/// coalesce (capacity 1). Deleted objects are reported as `ns/name` on
/// `deletes` when given.
fn watched<T>(
    client: &Client,
    cfg: watcher::Config,
    deletes: Option<tokio::sync::mpsc::UnboundedSender<String>>,
) -> (Store<T>, BoxStream<'static, ()>)
where
    T: Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync,
{
    let (reader, writer) = reflector::store::<T>();
    let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);
    let mut stream = Box::pin(
        reflector::reflector(writer, watcher(Api::<T>::all(client.clone()), cfg)).default_backoff(),
    );
    tokio::spawn(async move {
        while let Some(ev) = stream.next().await {
            match ev {
                Ok(watcher::Event::Delete(o)) => {
                    if let Some(d) = &deletes {
                        let _ = d.send(format!(
                            "{}/{}",
                            o.namespace().unwrap_or_default(),
                            o.name_any()
                        ));
                    }
                    let _ = tx.try_send(());
                }
                Ok(watcher::Event::Apply(_) | watcher::Event::InitDone) => {
                    let _ = tx.try_send(());
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "watch error"),
            }
        }
    });
    let triggers =
        futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|v| (v, rx)) })
            .boxed();
    (reader, triggers)
}

/// Builds every watcher, wires the controller and runs it until shutdown.
///
/// kube-runtime 4.2 gates `Controller::watches_stream` behind
/// `unstable-runtime-stream-control`. Instead every related kind (and the
/// gateways themselves) gets a spawned reflector that fills the `Store` read by
/// `reconcile`, and their post-apply triggers are merged into the stable
/// `Controller::reconcile_all_on`. Gateway deletions are not delivered to
/// `reconcile` by kube-runtime, so a small task cleans up from the deletion
/// events of the gateway reflector.
pub async fn run_controller(
    client: Client,
    ready: Readiness,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let cfg = watcher::Config::default;
    let controller = Controller::new(Api::<FeatherbitGateway>::all(client.clone()), cfg());

    let (del_tx, mut del_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (gateways, gateways_t) = watched::<FeatherbitGateway>(&client, cfg(), Some(del_tx));
    let (routes, routes_t) = watched::<Route>(&client, cfg(), None);
    let (policies, policies_t) = watched::<Policy>(&client, cfg(), None);
    let (supernodes, supernodes_t) = watched::<Supernode>(&client, cfg(), None);
    let (plugin_configs, plugin_configs_t) = watched::<PluginConfig>(&client, cfg(), None);
    let (stores, stores_t) = watched::<StoreCr>(&client, cfg(), None);
    let (consumers, consumers_t) = watched::<Consumer>(&client, cfg(), None);
    let (namespaces, namespaces_t) = watched::<Namespace>(&client, cfg(), None);
    let (configmaps, cms_t) = watched::<ConfigMap>(
        &client,
        watcher::Config::default().labels("app.kubernetes.io/managed-by=featherbit-operator"),
        None,
    );

    let ctx = Arc::new(Context {
        client: client.clone(),
        recorder: Recorder::new(
            client.clone(),
            Reporter {
                controller: "featherbit-operator".into(),
                instance: std::env::var("HOSTNAME").ok(),
            },
        ),
        gateways: gateways.clone(),
        namespaces: namespaces.clone(),
        routes: routes.clone(),
        policies: policies.clone(),
        supernodes: supernodes.clone(),
        plugin_configs: plugin_configs.clone(),
        stores: stores.clone(),
        consumers: consumers.clone(),
        configmaps: configmaps.clone(),
        etcd_applied: Default::default(),
        attempts: Default::default(),
        last_object_statuses: Default::default(),
        last_merged: Default::default(),
    });

    let mut sd = shutdown.clone();
    let controller = controller
        .reconcile_all_on(futures::stream::select_all([
            gateways_t,
            routes_t,
            policies_t,
            supernodes_t,
            plugin_configs_t,
            stores_t,
            consumers_t,
            namespaces_t,
            cms_t,
        ]))
        .graceful_shutdown_on(async move {
            let _ = sd.wait_for(|s| *s).await;
        });

    // Never reconcile against half-filled caches: an empty Route store would
    // render (and write) an empty config.
    let caches_ready = futures::future::join_all([
        gateways.wait_until_ready().boxed(),
        routes.wait_until_ready().boxed(),
        policies.wait_until_ready().boxed(),
        supernodes.wait_until_ready().boxed(),
        plugin_configs.wait_until_ready().boxed(),
        stores.wait_until_ready().boxed(),
        consumers.wait_until_ready().boxed(),
        namespaces.wait_until_ready().boxed(),
        configmaps.wait_until_ready().boxed(),
    ]);
    tokio::select! {
        _ = caches_ready => {}
        _ = shutdown.wait_for(|s| *s) => return Ok(()),
    }

    // A deleted gateway: forget its metrics and give objects only it selected a
    // final status (also covers the last gateway going away, which triggers no
    // reconcile of anything).
    let cleanup_ctx = ctx.clone();
    tokio::spawn(async move {
        while let Some(gone) = del_rx.recv().await {
            tracing::info!(gateway = %gone, "gateway deleted; cleaning up its objects");
            publish_objects(&cleanup_ctx).await;
        }
    });

    ready.0.store(true, Ordering::Relaxed);
    controller
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(gateway = %obj.name, "reconciled"),
                Err(e) => tracing::warn!(error = %e, "controller error"),
            }
        })
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::status::{ACCEPTED, REASON_VALID};

    fn obj() -> ObjectRef {
        ObjectRef {
            kind: Kind::Route,
            namespace: "ns".into(),
            name: "r".into(),
            uid: "u".into(),
            generation: 2,
            created: None,
        }
    }

    fn update(programmed: bool) -> MergedObjectStatus {
        MergedObjectStatus {
            obj: obj(),
            conditions: vec![
                cond(ACCEPTED, true, REASON_VALID, "", 2),
                cond(PROGRAMMED, programmed, REASON_PROGRAMMED, "", 2),
            ],
            gateways: vec![],
        }
    }

    fn aged(mut s: ResourceStatus) -> ResourceStatus {
        // An older timestamp than any `cond()` call in the next pass will produce.
        for c in &mut s.conditions {
            c.last_transition_time = Time(k8s_openapi::jiff::Timestamp::UNIX_EPOCH);
        }
        s
    }

    #[test]
    fn next_status_same_conditions_twice_is_unchanged_and_keeps_transition_time() {
        let (first, changed) = next_status(None, &update(true));
        assert!(changed, "first status must be written");
        let stored = aged(first);
        let (second, changed) = next_status(Some(&stored), &update(true));
        assert!(!changed, "identical verdicts must not patch");
        assert_eq!(second, stored);
        assert_eq!(
            second.conditions[0].last_transition_time,
            Time(k8s_openapi::jiff::Timestamp::UNIX_EPOCH)
        );
    }

    #[test]
    fn next_status_status_flip_is_changed_and_stamps_only_the_flipped_condition() {
        let stored = aged(next_status(None, &update(true)).0);
        let (next, changed) = next_status(Some(&stored), &update(false));
        assert!(changed);
        let accepted = next
            .conditions
            .iter()
            .find(|c| c.type_ == ACCEPTED)
            .unwrap();
        let programmed = next
            .conditions
            .iter()
            .find(|c| c.type_ == PROGRAMMED)
            .unwrap();
        assert_eq!(programmed.status, "False");
        assert_eq!(
            accepted.last_transition_time,
            Time(k8s_openapi::jiff::Timestamp::UNIX_EPOCH)
        );
        assert_ne!(
            programmed.last_transition_time,
            Time(k8s_openapi::jiff::Timestamp::UNIX_EPOCH)
        );
    }

    #[test]
    fn backoff_doubles_from_30s_and_caps_at_300s() {
        let secs: Vec<u64> = (0..6).map(|n| backoff_delay(n).as_secs()).collect();
        assert_eq!(secs, [30, 60, 120, 240, 300, 300]);
        assert_eq!(backoff_delay(u32::MAX).as_secs(), 300);
    }

    #[test]
    fn merge_conditions_drops_types_that_are_no_longer_planned() {
        let current = vec![
            cond(ACCEPTED, true, REASON_VALID, "", 1),
            cond(PROGRAMMED, true, REASON_PROGRAMMED, "", 1),
        ];
        let merged = merge_conditions(
            &current,
            &[cond(PROGRAMMED, true, REASON_PROGRAMMED, "", 2)],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].type_, PROGRAMMED);
    }
}
