//! The reconcile pipeline. `select`, `verdict`, `render` and `plan` are pure
//! and unit-tested; `status`, the sinks and the controller in this file do IO.
pub mod plan;
pub mod render;
pub mod select;
pub mod status;
pub mod verdict;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{FutureExt, StreamExt};
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::events::{Recorder, Reporter};
use kube::runtime::reflector::{self, ObjectRef as KubeObjectRef, Store};
use kube::runtime::{watcher, WatchStreamExt};
use kube::{Api, Client, Resource, ResourceExt};

use crate::crd::gateway::{FeatherbitGateway, GatewayStatus};
use crate::crd::resources::{
    Consumer, Kind, PluginConfig, Policy, Route, Store as StoreCr, Supernode,
};
use crate::crd::status::{cond, set_condition, PROGRAMMED, READY, REASON_SINK_UNAVAILABLE};
use crate::reconcile::plan::{merge_object_statuses, plan, GatewayId, Input, ObjectStatusUpdate};
use crate::reconcile::select::{candidate, Candidate};
use crate::sink::{self, Target};
use crate::telemetry::{self, Readiness};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kube: {0}")]
    Kube(#[from] kube::Error),
    #[error("sink: {0}")]
    Sink(#[from] sink::SinkError),
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
    /// Last per-gateway object statuses, for the cross-gateway merge.
    pub last_object_statuses:
        tokio::sync::Mutex<std::collections::BTreeMap<String, Vec<ObjectStatusUpdate>>>,
}

impl Context {
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
}

pub const REQUEUE_OK: Duration = Duration::from_secs(600);

pub async fn reconcile(gw: Arc<FeatherbitGateway>, ctx: Arc<Context>) -> Result<Action, Error> {
    let started = Instant::now();
    let ns = gw.namespace().unwrap_or_default();
    let name = gw.name_any();
    let key = format!("{ns}/{name}");
    let generation = gw.metadata.generation.unwrap_or(0);
    let previous_hash = gw.status.as_ref().and_then(|s| s.config_hash.clone());

    let p = plan(Input {
        gateway: GatewayId {
            namespace: ns.clone(),
            name: name.clone(),
            generation,
        },
        spec: gw.spec.clone(),
        namespaces: ctx
            .namespaces
            .state()
            .iter()
            .map(|n| (**n).clone())
            .collect(),
        candidates: ctx.candidates(),
        previous_hash,
    });

    // Sink write (spec §5 step 8): only when the plan says the hash changed.
    let mut gateway_conditions = p.gateway_conditions.clone();
    let hash = p.rendered.as_ref().map(|r| r.hash.clone());
    let mut result = "ok";
    if let Some(rendered) = &p.write {
        let target = Target::from_spec(&ns, &name, &gw.spec);
        if let Err(e) = sink::write(&ctx.client, &target, rendered, &p.config).await {
            tracing::error!(gateway = %key, error = %e, "sink write failed");
            set_condition(
                &mut gateway_conditions,
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
                conditions: gateway_conditions,
                observed_generation: gw.metadata.generation,
                config_hash: gw.status.as_ref().and_then(|s| s.config_hash.clone()),
                last_rendered_at: gw.status.as_ref().and_then(|s| s.last_rendered_at.clone()),
                counts: p.counts.clone(),
            };
            if gw.status.as_ref() != Some(&status) {
                status::patch_gateway(&ctx.client, &ns, &name, &status).await?;
            }
            telemetry::reconcile_observed(&key, "sink_error", started.elapsed().as_secs_f64());
            return Err(Error::Sink(e));
        }
    }
    if let Some(h) = &hash {
        telemetry::rendered_hash(&key, h);
    }
    if p.write.is_none()
        && gateway_conditions
            .iter()
            .any(|c| c.type_ == READY && c.status == "False")
    {
        result = "not_ready";
    }

    // Gateway status; patched only when it differs from what is stored.
    let last_rendered_at = if p.write.is_some() {
        Some(Time(k8s_openapi::jiff::Timestamp::now()))
    } else {
        gw.status.as_ref().and_then(|s| s.last_rendered_at.clone())
    };
    let status = GatewayStatus {
        conditions: gateway_conditions,
        observed_generation: gw.metadata.generation,
        config_hash: hash,
        last_rendered_at,
        counts: p.counts.clone(),
    };
    if gw.status.as_ref() != Some(&status) {
        status::patch_gateway(&ctx.client, &ns, &name, &status).await?;
    }

    // Object statuses: merge this gateway's verdicts with the other gateways' last ones.
    let merged = {
        let mut last = ctx.last_object_statuses.lock().await;
        last.insert(key.clone(), p.object_statuses.clone());
        let live: std::collections::BTreeSet<String> = ctx
            .gateways
            .state()
            .iter()
            .map(|g| format!("{}/{}", g.namespace().unwrap_or_default(), g.name_any()))
            .collect();
        last.retain(|k, _| live.contains(k));
        merge_object_statuses(last.values().cloned().collect())
    };
    for m in &merged {
        if let Err(e) = status::patch_object(&ctx.client, m).await {
            tracing::warn!(object = %m.obj.display(), error = %e, "status patch failed");
        }
    }

    // Events and metrics.
    let gw_ref = ObjectReference {
        api_version: Some("featherbit.io/v1alpha1".into()),
        kind: Some("FeatherbitGateway".into()),
        name: Some(name.clone()),
        namespace: Some(ns.clone()),
        uid: gw.uid(),
        ..Default::default()
    };
    for e in &p.events {
        let target = e
            .obj
            .as_ref()
            .map(status::object_reference)
            .unwrap_or_else(|| gw_ref.clone());
        status::emit(&ctx.recorder, &target, e).await;
    }
    let mut per_reason: std::collections::BTreeMap<(String, String), u64> = Default::default();
    for o in &p.object_statuses {
        for c in &o.conditions {
            if c.status == "False" && c.type_ != PROGRAMMED {
                *per_reason
                    .entry((o.obj.kind.kind_str().to_string(), c.reason.clone()))
                    .or_default() += 1;
            }
        }
    }
    telemetry::excluded_clear(&key);
    for ((kind, reason), n) in per_reason {
        telemetry::excluded_set(&key, &kind, &reason, n);
    }
    telemetry::reconcile_observed(&key, result, started.elapsed().as_secs_f64());
    Ok(Action::requeue(REQUEUE_OK))
}

pub fn error_policy(_gw: Arc<FeatherbitGateway>, err: &Error, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "reconcile failed; backing off");
    Action::requeue(Duration::from_secs(30))
}

/// Spawns a reflector task that keeps a `Store` fresh for `T`.
fn watched<T>(client: &Client, cfg: watcher::Config) -> Store<T>
where
    T: Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    T::DynamicType: Default + Clone + Eq + std::hash::Hash + Send + Sync,
{
    let (reader, writer) = reflector::store::<T>();
    let stream = reflector::reflector(writer, watcher(Api::<T>::all(client.clone()), cfg))
        .default_backoff()
        .touched_objects();
    tokio::spawn(stream.for_each(|_| async {}));
    reader
}

/// Builds every watcher, wires the controller and runs it until shutdown.
///
/// kube-runtime 4.2 gates `Controller::watches_stream` behind
/// `unstable-runtime-stream-control`, so each related kind is watched twice:
/// `Controller::watches` maps events to every gateway, and a spawned reflector
/// fills the `Store` that `reconcile` reads.
pub async fn run_controller(
    client: Client,
    ready: Readiness,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let cfg = watcher::Config::default;
    let controller = Controller::new(Api::<FeatherbitGateway>::all(client.clone()), cfg());
    let gateways = controller.store();
    macro_rules! all_gateways {
        () => {{
            let gateways = gateways.clone();
            move |_| -> Vec<KubeObjectRef<FeatherbitGateway>> {
                gateways
                    .state()
                    .iter()
                    .map(|g| KubeObjectRef::from_obj(g.as_ref()))
                    .collect()
            }
        }};
    }

    let routes = watched::<Route>(&client, cfg());
    let policies = watched::<Policy>(&client, cfg());
    let supernodes = watched::<Supernode>(&client, cfg());
    let plugin_configs = watched::<PluginConfig>(&client, cfg());
    let stores = watched::<StoreCr>(&client, cfg());
    let consumers = watched::<Consumer>(&client, cfg());
    let namespaces = watched::<Namespace>(&client, cfg());
    let cm_cfg =
        || watcher::Config::default().labels("app.kubernetes.io/managed-by=featherbit-operator");

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
        last_object_statuses: Default::default(),
    });

    let mut sd = shutdown.clone();
    let controller = controller
        .watches(Api::<Route>::all(client.clone()), cfg(), all_gateways!())
        .watches(Api::<Policy>::all(client.clone()), cfg(), all_gateways!())
        .watches(
            Api::<Supernode>::all(client.clone()),
            cfg(),
            all_gateways!(),
        )
        .watches(
            Api::<PluginConfig>::all(client.clone()),
            cfg(),
            all_gateways!(),
        )
        .watches(Api::<StoreCr>::all(client.clone()), cfg(), all_gateways!())
        .watches(Api::<Consumer>::all(client.clone()), cfg(), all_gateways!())
        .watches(
            Api::<Namespace>::all(client.clone()),
            cfg(),
            all_gateways!(),
        )
        .watches(
            Api::<ConfigMap>::all(client.clone()),
            cm_cfg(),
            all_gateways!(),
        )
        .graceful_shutdown_on(async move {
            let _ = sd.wait_for(|s| *s).await;
        });

    // Never reconcile against half-filled caches: an empty Route store would
    // render (and write) an empty config.
    let caches_ready = futures::future::join_all([
        routes.wait_until_ready().boxed(),
        policies.wait_until_ready().boxed(),
        supernodes.wait_until_ready().boxed(),
        plugin_configs.wait_until_ready().boxed(),
        stores.wait_until_ready().boxed(),
        consumers.wait_until_ready().boxed(),
        namespaces.wait_until_ready().boxed(),
    ]);
    tokio::select! {
        _ = caches_ready => {}
        _ = shutdown.wait_for(|s| *s) => return Ok(()),
    }

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
