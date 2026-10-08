//! `featherbit-operator run`: process wiring.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use futures::future::FusedFuture;
use futures::FutureExt;

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
    let tx = Arc::new(tx);
    tokio::spawn({
        let tx = tx.clone();
        async move {
            shutdown_signal().await;
            tracing::info!("shutdown signal received");
            let _ = tx.send(true);
        }
    });
    let ready = Readiness(Arc::new(AtomicBool::new(false)));

    let mut metrics = tokio::spawn(telemetry::serve(
        cfg.metrics_addr,
        ready.clone(),
        rx.clone(),
    ))
    .map(flatten)
    .fuse();
    let mut webhook = tokio::spawn({
        let rx = rx.clone();
        async move {
            crate::webhook::serve(cfg.webhook_addr, &cfg.tls_cert, &cfg.tls_key, rx).await
        }
    })
    .map(flatten)
    .fuse();
    let mut controller = Box::pin(crate::reconcile::run_controller(client, ready, rx)).fuse();

    // The first component to end (error or not) takes the process down.
    let first: anyhow::Result<()> = tokio::select! {
        r = &mut controller => r,
        r = &mut webhook => r,
        r = &mut metrics => r,
    };
    if let Err(e) = &first {
        tracing::error!(error = %format!("{e:#}"), "component failed; shutting down");
    }
    let _ = tx.send(true);
    // Drain the others (bounded) so shutdown is graceful.
    let drain = async {
        let mut rest: anyhow::Result<()> = Ok(());
        if !controller.is_terminated() {
            rest = rest.and(controller.await);
        }
        if !webhook.is_terminated() {
            rest = rest.and(webhook.await);
        }
        if !metrics.is_terminated() {
            rest = rest.and(metrics.await);
        }
        rest
    };
    let rest = tokio::time::timeout(std::time::Duration::from_secs(15), drain)
        .await
        .unwrap_or(Ok(()));
    first.and(rest)?;
    tracing::info!("shutdown complete");
    Ok(())
}

fn flatten(r: Result<anyhow::Result<()>, tokio::task::JoinError>) -> anyhow::Result<()> {
    match r {
        Ok(inner) => inner,
        Err(e) => Err(anyhow::anyhow!("task failed: {e}")),
    }
}
