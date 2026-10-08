//! Status patches and Events (IO).

use k8s_openapi::api::core::v1::ObjectReference;
use kube::api::{Patch, PatchParams};
use kube::runtime::events::{Event, EventType, Recorder};
use kube::{Api, Client};
use serde_json::json;

use crate::crd::gateway::{FeatherbitGateway, GatewayStatus};
use crate::crd::resources::{Consumer, Kind, PluginConfig, Policy, Route, Store, Supernode};
use crate::crd::status::ResourceStatus;
use crate::reconcile::plan::{EventSpec, MergedObjectStatus};
use crate::reconcile::select::ObjectRef;
use crate::sink::configmap::FIELD_MANAGER;

pub async fn patch_gateway(
    client: &Client,
    ns: &str,
    name: &str,
    status: &GatewayStatus,
) -> Result<(), kube::Error> {
    let body = json!({
        "apiVersion": "featherbit.io/v1alpha1",
        "kind": "FeatherbitGateway",
        "metadata": { "name": name, "namespace": ns },
        "status": status,
    });
    Api::<FeatherbitGateway>::namespaced(client.clone(), ns)
        .patch_status(
            name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&body),
        )
        .await
        .map(|_| ())
}

pub async fn patch_object(client: &Client, m: &MergedObjectStatus) -> Result<(), kube::Error> {
    let status = ResourceStatus {
        conditions: m.conditions.clone(),
        observed_generation: Some(m.obj.generation),
        gateways: m.gateways.clone(),
    };
    let body = json!({
        "apiVersion": "featherbit.io/v1alpha1",
        "kind": m.obj.kind.kind_str(),
        "metadata": { "name": m.obj.name, "namespace": m.obj.namespace },
        "status": status,
    });
    let pp = PatchParams::apply(FIELD_MANAGER).force();
    let ns = &m.obj.namespace;
    let name = &m.obj.name;
    let patch = Patch::Apply(&body);
    match m.obj.kind {
        Kind::Route => Api::<Route>::namespaced(client.clone(), ns)
            .patch_status(name, &pp, &patch)
            .await
            .map(|_| ()),
        Kind::Policy => Api::<Policy>::namespaced(client.clone(), ns)
            .patch_status(name, &pp, &patch)
            .await
            .map(|_| ()),
        Kind::Supernode => Api::<Supernode>::namespaced(client.clone(), ns)
            .patch_status(name, &pp, &patch)
            .await
            .map(|_| ()),
        Kind::PluginConfig => Api::<PluginConfig>::namespaced(client.clone(), ns)
            .patch_status(name, &pp, &patch)
            .await
            .map(|_| ()),
        Kind::Store => Api::<Store>::namespaced(client.clone(), ns)
            .patch_status(name, &pp, &patch)
            .await
            .map(|_| ()),
        Kind::Consumer => Api::<Consumer>::namespaced(client.clone(), ns)
            .patch_status(name, &pp, &patch)
            .await
            .map(|_| ()),
    }
}

pub fn object_reference(obj: &ObjectRef) -> ObjectReference {
    ObjectReference {
        api_version: Some("featherbit.io/v1alpha1".into()),
        kind: Some(obj.kind.kind_str().into()),
        name: Some(obj.name.clone()),
        namespace: Some(obj.namespace.clone()),
        uid: Some(obj.uid.clone()),
        ..Default::default()
    }
}

pub async fn emit(recorder: &Recorder, target: &ObjectReference, e: &EventSpec) {
    let event = Event {
        type_: if e.warning {
            EventType::Warning
        } else {
            EventType::Normal
        },
        reason: e.reason.clone(),
        note: Some(e.message.clone()),
        action: "Reconcile".into(),
        secondary: None,
    };
    if let Err(err) = recorder.publish(&event, target).await {
        tracing::warn!(error = %err, "event publish failed");
    }
}
