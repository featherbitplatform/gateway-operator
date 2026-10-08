//! Admission webhook HTTP server (axum over rustls).
pub mod admit;

use std::net::SocketAddr;
use std::path::Path;

use axum::extract::Path as AxPath;
use axum::routing::{get, post};
use axum::{Json, Router};
use kube::core::admission::{AdmissionRequest, AdmissionResponse, AdmissionReview};
use kube::core::DynamicObject;
use kube::ResourceExt;

pub fn router() -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/validate/featherbit.io/v1alpha1/{kind}", post(validate))
}

const KINDS: [&str; 7] = [
    "Route",
    "Policy",
    "Supernode",
    "PluginConfig",
    "Store",
    "Consumer",
    "FeatherbitGateway",
];

/// Canonical spelling of a kind. The webhook URL carries the lowercase form
/// (the API server rejects uppercase path segments); unknown kinds pass through.
fn canonical_kind(kind: &str) -> &str {
    KINDS
        .iter()
        .find(|k| k.eq_ignore_ascii_case(kind))
        .copied()
        .unwrap_or(kind)
}

/// Metric label for a kind: the seven known kinds, else `unknown`, so URL
/// paths and request bodies cannot grow label cardinality.
fn metric_kind(kind: &str) -> &str {
    KINDS
        .iter()
        .find(|k| **k == kind)
        .copied()
        .unwrap_or("unknown")
}

async fn validate(
    AxPath(kind): AxPath<String>,
    Json(review): Json<AdmissionReview<DynamicObject>>,
) -> Json<AdmissionReview<DynamicObject>> {
    let req: AdmissionRequest<DynamicObject> = match review.try_into() {
        Ok(r) => r,
        Err(e) => return Json(AdmissionResponse::invalid(e.to_string()).into_review()),
    };
    let mut res = AdmissionResponse::from(&req);
    if let Some(obj) = &req.object {
        let name = obj.name_any();
        let spec = obj
            .data
            .get("spec")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let kind = if req.kind.kind.is_empty() {
            canonical_kind(&kind).to_string()
        } else {
            req.kind.kind.clone()
        };
        let label = metric_kind(&kind);
        if let Err(msg) = admit::admit(&kind, &name, &spec) {
            crate::telemetry::webhook_observed(label, false);
            res = res.deny(msg);
        } else {
            crate::telemetry::webhook_observed(label, true);
        }
    }
    Json(res.into_review())
}

/// Serves the router over TLS until `shutdown` resolves. Re-reads the PEM
/// files hourly so a cert-manager rotation is picked up without a restart.
pub async fn serve(
    addr: SocketAddr,
    cert: &Path,
    key: &Path,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
    let reload = config.clone();
    let (cert, key) = (cert.to_path_buf(), key.to_path_buf());
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            if let Err(e) = reload.reload_from_pem_file(&cert, &key).await {
                tracing::warn!(error = %e, "webhook certificate reload failed");
            }
        }
    });
    let handle = axum_server::Handle::new();
    let h = handle.clone();
    let mut shutdown = shutdown;
    tokio::spawn(async move {
        let _ = shutdown.wait_for(|s| *s).await;
        h.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
    });
    axum_server::bind_rustls(addr, config)
        .handle(handle)
        .serve(router().into_make_service())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn review(kind: &str, spec: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "123",
                "kind": {"group": "featherbit.io", "version": "v1alpha1", "kind": kind},
                "resource": {"group": "featherbit.io", "version": "v1alpha1", "resource": kind.to_lowercase()},
                "name": "obj", "namespace": "ns", "operation": "CREATE", "userInfo": {},
                "object": {"apiVersion": "featherbit.io/v1alpha1", "kind": kind, "metadata": {"name": "obj", "namespace": "ns"}, "spec": spec}
            }
        })
    }

    async fn post(kind: &str, spec: serde_json::Value) -> serde_json::Value {
        // The chart registers the lowercase path; the review body carries the real kind.
        let req = Request::post(format!(
            "/validate/featherbit.io/v1alpha1/{}",
            kind.to_lowercase()
        ))
        .header("content-type", "application/json")
        .body(Body::from(review(kind, spec).to_string()))
        .unwrap();
        let res = router().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn canonical_kind_restores_case() {
        assert_eq!(canonical_kind("pluginconfig"), "PluginConfig");
        assert_eq!(canonical_kind("featherbitgateway"), "FeatherbitGateway");
        assert_eq!(canonical_kind("bogus"), "bogus");
    }

    #[test]
    fn metric_kind_collapses_unknown_kinds() {
        assert_eq!(metric_kind("Route"), "Route");
        assert_eq!(metric_kind("FeatherbitGateway"), "FeatherbitGateway");
        assert_eq!(metric_kind("Whatever"), "unknown");
        assert_eq!(metric_kind(""), "unknown");
    }

    #[tokio::test]
    async fn denies_and_allows_with_the_request_uid() {
        let denied = post(
            "Route",
            serde_json::json!({"match": {"path": "/a", "host": "*"}, "policy": "p"}),
        )
        .await;
        assert_eq!(denied["response"]["uid"], "123");
        assert_eq!(denied["response"]["allowed"], false);
        assert!(denied["response"]["status"]["message"]
            .as_str()
            .unwrap()
            .contains("Route obj"));
        let allowed = post(
            "Route",
            serde_json::json!({"match": {"path": "/a"}, "policy": "p"}),
        )
        .await;
        assert_eq!(allowed["response"]["allowed"], true);
    }
}
