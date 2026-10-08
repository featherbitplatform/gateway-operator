//! Spec §7: where a rendered config goes.
pub mod configmap;
pub mod etcd;

use featherbit::config::GatewayConfig;
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};

use crate::crd::gateway::{EtcdSink, FeatherbitGatewaySpec};
use crate::reconcile::render::Rendered;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SinkError(pub String);

#[derive(Clone, Debug)]
pub enum Target {
    ConfigMap {
        namespace: String,
        name: String,
        gateway: String,
    },
    Etcd {
        namespace: String,
        sink: EtcdSink,
    },
}

impl Target {
    /// `validate_spec` has already guaranteed exactly one sink.
    pub fn from_spec(gateway_ns: &str, gateway_name: &str, spec: &FeatherbitGatewaySpec) -> Target {
        if let Some(cm) = &spec.sink.config_map {
            Target::ConfigMap {
                namespace: gateway_ns.into(),
                name: cm.name.clone(),
                gateway: gateway_name.into(),
            }
        } else {
            Target::Etcd {
                namespace: gateway_ns.into(),
                sink: spec.sink.etcd.clone().expect("one sink"),
            }
        }
    }
}

pub async fn write(
    client: &Client,
    target: &Target,
    rendered: &Rendered,
    config: &GatewayConfig,
) -> Result<(), SinkError> {
    match target {
        Target::ConfigMap {
            namespace,
            name,
            gateway,
        } => {
            let cm = configmap::desired_configmap(namespace, name, gateway, rendered);
            configmap::apply(client, &cm).await
        }
        Target::Etcd { namespace, sink } => {
            let secret = match &sink.credentials_secret_ref {
                None => None,
                Some(r) => Some(
                    Api::<Secret>::namespaced(client.clone(), namespace)
                        .get(&r.name)
                        .await
                        .map_err(|e| {
                            SinkError(format!("reading Secret {namespace}/{}: {e}", r.name))
                        })?,
                ),
            };
            let cfg = etcd::etcd_config(sink, secret.as_ref())?;
            etcd::write(&cfg, config).await
        }
    }
}
