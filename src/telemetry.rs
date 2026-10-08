//! Spec §9: metrics, health endpoints, logging.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};

struct Metrics {
    registry: Registry,
    reconcile_total: IntCounterVec,
    reconcile_duration: HistogramVec,
    excluded: GaugeVec,
    rendered: GaugeVec,
    webhook: IntCounterVec,
    last_hash: Mutex<HashMap<String, String>>,
    /// Label sets currently exported on `excluded`, per gateway, so series
    /// that stop applying can be removed instead of lingering at a stale value.
    excluded_keys: Mutex<HashMap<String, BTreeSet<(String, String)>>>,
}

fn metrics() -> &'static Metrics {
    static M: OnceLock<Metrics> = OnceLock::new();
    M.get_or_init(|| {
        let registry = Registry::new();
        let reconcile_total = IntCounterVec::new(
            Opts::new(
                "featherbit_operator_reconcile_total",
                "Reconciles per gateway and result",
            ),
            &["gateway", "result"],
        )
        .unwrap();
        let reconcile_duration = HistogramVec::new(
            HistogramOpts::new(
                "featherbit_operator_reconcile_duration_seconds",
                "Reconcile duration",
            ),
            &["gateway"],
        )
        .unwrap();
        let excluded = GaugeVec::new(
            Opts::new(
                "featherbit_operator_excluded_objects",
                "Objects excluded from a gateway's render",
            ),
            &["gateway", "kind", "reason"],
        )
        .unwrap();
        let rendered = GaugeVec::new(
            Opts::new(
                "featherbit_operator_rendered_config_info",
                "1 for the hash currently rendered per gateway",
            ),
            &["gateway", "hash"],
        )
        .unwrap();
        let webhook = IntCounterVec::new(
            Opts::new(
                "featherbit_operator_webhook_requests_total",
                "Admission requests per kind and verdict",
            ),
            &["kind", "allowed"],
        )
        .unwrap();
        for c in [&reconcile_total, &webhook] {
            registry.register(Box::new(c.clone())).unwrap();
        }
        registry
            .register(Box::new(reconcile_duration.clone()))
            .unwrap();
        registry.register(Box::new(excluded.clone())).unwrap();
        registry.register(Box::new(rendered.clone())).unwrap();
        Metrics {
            registry,
            reconcile_total,
            reconcile_duration,
            excluded,
            rendered,
            webhook,
            last_hash: Mutex::new(Default::default()),
            excluded_keys: Mutex::new(Default::default()),
        }
    })
}

pub fn reconcile_observed(gateway: &str, result: &str, secs: f64) {
    let m = metrics();
    m.reconcile_total
        .with_label_values(&[gateway, result])
        .inc();
    m.reconcile_duration
        .with_label_values(&[gateway])
        .observe(secs);
}

pub fn excluded_set(gateway: &str, kind: &str, reason: &str, n: u64) {
    let m = metrics();
    m.excluded_keys
        .lock()
        .unwrap()
        .entry(gateway.to_string())
        .or_default()
        .insert((kind.to_string(), reason.to_string()));
    m.excluded
        .with_label_values(&[gateway, kind, reason])
        .set(n as f64);
}

/// Drops every `excluded_objects` series of a gateway; call before re-setting
/// the current counts so reasons that no longer apply disappear.
pub fn excluded_clear(gateway: &str) {
    let m = metrics();
    if let Some(keys) = m.excluded_keys.lock().unwrap().remove(gateway) {
        for (kind, reason) in keys {
            let _ = m.excluded.remove_label_values(&[gateway, &kind, &reason]);
        }
    }
}

pub fn rendered_hash(gateway: &str, hash: &str) {
    let m = metrics();
    let mut last = m.last_hash.lock().unwrap();
    if let Some(old) = last.insert(gateway.to_string(), hash.to_string()) {
        if old != hash {
            let _ = m.rendered.remove_label_values(&[gateway, &old]);
        }
    }
    m.rendered.with_label_values(&[gateway, hash]).set(1.0);
}

/// Removes every series of a gateway that no longer exists.
pub fn forget_gateway(gateway: &str) {
    let m = metrics();
    if let Some(old) = m.last_hash.lock().unwrap().remove(gateway) {
        let _ = m.rendered.remove_label_values(&[gateway, &old]);
    }
    excluded_clear(gateway);
}

pub fn webhook_observed(kind: &str, allowed: bool) {
    metrics()
        .webhook
        .with_label_values(&[kind, if allowed { "true" } else { "false" }])
        .inc();
}

pub fn render_metrics() -> String {
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&metrics().registry.gather(), &mut buf)
        .unwrap();
    String::from_utf8(buf).unwrap()
}

pub fn init_logging(format: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    if format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

#[derive(Clone)]
pub struct Readiness(pub Arc<AtomicBool>);

pub async fn serve(
    addr: std::net::SocketAddr,
    ready: Readiness,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/metrics", get(|| async { render_metrics() }))
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get(|State(r): State<Readiness>| async move {
                if r.0.load(Ordering::Relaxed) {
                    (StatusCode::OK, "ready")
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "not ready")
                }
            }),
        )
        .with_state(ready);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.wait_for(|s| *s).await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_render_with_spec_names() {
        reconcile_observed("gw/edge", "ok", 0.25);
        excluded_set("gw/edge", "Route", "PolicyNotFound", 2);
        rendered_hash("gw/edge", "abc");
        rendered_hash("gw/edge", "def"); // old hash series must disappear
        webhook_observed("Policy", false);
        let text = render_metrics();
        for name in [
            "featherbit_operator_reconcile_total",
            "featherbit_operator_reconcile_duration_seconds",
            "featherbit_operator_excluded_objects",
            "featherbit_operator_rendered_config_info",
            "featherbit_operator_webhook_requests_total",
        ] {
            assert!(text.contains(name), "{name} missing in:\n{text}");
        }
        assert!(
            text.contains(r#"hash="def""#) && !text.contains(r#"hash="abc""#),
            "{text}"
        );
        assert!(text.contains(r#"allowed="false""#));
    }
}
