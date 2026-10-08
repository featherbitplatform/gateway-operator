//! `featherbit-operator run`: process wiring.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::telemetry::{self, Readiness};

pub struct RunConfig {
    pub tls_cert: std::path::PathBuf,
    pub tls_key: std::path::PathBuf,
    pub webhook_addr: std::net::SocketAddr,
    pub metrics_addr: std::net::SocketAddr,
    pub log_format: String,
}

/// Resolves on Ctrl-C or, on Unix, SIGTERM (what Kubernetes sends).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => ctrl_c.await,
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

pub async fn run(cfg: RunConfig) -> anyhow::Result<()> {
    telemetry::init_logging(&cfg.log_format);
    // axum-server is built with `tls-rustls-no-provider`; install ring first.
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    tracing::info!("featherbit-operator {} starting", env!("CARGO_PKG_VERSION"));

    let client = kube::Client::try_default().await?;
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received");
        let _ = tx.send(true);
    });
    let ready = Readiness(Arc::new(AtomicBool::new(false)));

    let metrics = tokio::spawn(telemetry::serve(
        cfg.metrics_addr,
        ready.clone(),
        rx.clone(),
    ));
    let webhook = tokio::spawn({
        let rx = rx.clone();
        async move { crate::webhook::serve(cfg.webhook_addr, &cfg.tls_cert, &cfg.tls_key, rx).await }
    });
    crate::reconcile::run_controller(client, ready, rx).await?;
    webhook.await??;
    metrics.await??;
    tracing::info!("shutdown complete");
    Ok(())
}
