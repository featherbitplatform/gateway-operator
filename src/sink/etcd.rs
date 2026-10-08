//! Spec §7.2: the etcd sink, through the gateway's own store code.

use featherbit::config::{EtcdConfig, GatewayConfig};
use k8s_openapi::api::core::v1::Secret;

use super::SinkError;
use crate::crd::gateway::EtcdSink;

fn secret_str(secret: &Secret, name: &str, key: &str) -> Result<String, SinkError> {
    let bytes = secret
        .data
        .as_ref()
        .and_then(|d| d.get(key))
        .ok_or_else(|| SinkError(format!("Secret '{name}' has no '{key}' key")))?;
    String::from_utf8(bytes.0.clone())
        .map_err(|_| SinkError(format!("Secret '{name}' key '{key}' is not UTF-8")))
}

pub fn etcd_config(sink: &EtcdSink, secret: Option<&Secret>) -> Result<EtcdConfig, SinkError> {
    let (user, password) = match (&sink.credentials_secret_ref, secret) {
        (None, _) => (None, None),
        (Some(r), None) => return Err(SinkError(format!("Secret '{}' not found", r.name))),
        (Some(r), Some(s)) => (
            Some(secret_str(s, &r.name, "user")?),
            Some(secret_str(s, &r.name, "password")?),
        ),
    };
    // Build through serde so the gateway's own defaults and any future field apply.
    let value = serde_json::json!({
        "endpoints": sink.endpoints,
        "prefix": sink.prefix,
        "timeout_ms": sink.timeout_ms,
        "user": user,
        "password": password,
    });
    serde_json::from_value(value).map_err(|e| SinkError(format!("etcd config: {e}")))
}

pub async fn write(cfg: &EtcdConfig, config: &GatewayConfig) -> Result<(), SinkError> {
    featherbit::config_store::etcd::reconcile_prefix(cfg, config)
        .await
        .map_err(|e| SinkError(format!("etcd {}: {e}", cfg.endpoints.join(","))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::Secret;
    use k8s_openapi::ByteString;

    fn sink(yaml: &str) -> EtcdSink {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn secret(keys: &[(&str, &str)]) -> Secret {
        Secret {
            data: Some(
                keys.iter()
                    .map(|(k, v)| (k.to_string(), ByteString(v.as_bytes().to_vec())))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn etcd_config_without_credentials() {
        let cfg = etcd_config(
            &sink("endpoints: ['http://a:2379']\nprefix: /p\ntimeoutMs: 500"),
            None,
        )
        .unwrap();
        assert_eq!(cfg.endpoints, ["http://a:2379"]);
        assert_eq!(cfg.prefix, "/p");
        assert_eq!(cfg.timeout_ms, 500);
        assert!(cfg.user.is_none() && cfg.password.is_none());
    }

    #[test]
    fn etcd_config_reads_user_and_password_from_the_secret() {
        let s = sink("endpoints: ['http://a:2379']\ncredentialsSecretRef: { name: creds }");
        let cfg = etcd_config(&s, Some(&secret(&[("user", "root"), ("password", "pw")]))).unwrap();
        assert_eq!(cfg.user.as_deref(), Some("root"));
        assert_eq!(cfg.password.as_deref(), Some("pw"));
    }

    #[test]
    fn etcd_config_from_spec_reports_missing_secret_key() {
        let s = sink("endpoints: ['http://a:2379']\ncredentialsSecretRef: { name: creds }");
        let err = etcd_config(&s, Some(&secret(&[("user", "root")]))).unwrap_err();
        assert!(
            err.to_string().contains("creds") && err.to_string().contains("password"),
            "{err}"
        );
        let err = etcd_config(&s, None).unwrap_err();
        assert!(err.to_string().contains("creds"), "{err}");
    }
}
