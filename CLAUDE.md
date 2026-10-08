# CLAUDE.md

Kubernetes operator for the featherbit API gateway (`featherbitplatform/gateway`). Design: `docs/superpowers/specs/2026-10-08-operator-design.md`.

## Conventions
- Gitflow (`develop`, `feature/*`, `release/*`, `hotfix/*`) and Conventional Commits, same as the gateway. No `Co-Authored-By` trailers.
- The `featherbit` crate is a git dependency pinned in `Cargo.toml`; its public surface is pinned by the gateway's `tests/lib_surface.rs`. Validation logic is never reimplemented here — call the gateway's validators.

## Commands
```bash
cargo test --locked                                  # unit + CLI tests
cargo clippy --all-targets --locked -- -D warnings
cargo run -- crds > charts/featherbit-operator/crds/featherbit.io.yaml   # regenerate CRDs (CI checks they match)
helm lint --strict charts/featherbit-operator
bash e2e/run.sh                                      # kind-based end-to-end (needs docker, kind, helm, kubectl)
```

## Layout
- `src/crd/` CRD types (`resources.rs` six kinds, `gateway.rs` binding, `status.rs` conditions, `schema.rs` gateway-type schemas)
- `src/validators.rs` per-object checks shared by webhook and reconciler
- `src/reconcile/` pure planning pipeline (`select`, `verdict`, `render`, `plan`) + `status` IO + controller wiring in `mod.rs`
- `src/sink/` ConfigMap and etcd writers
- `src/webhook/` admission server
- `src/telemetry.rs` metrics + health server
- `charts/featherbit-operator/` Helm chart (CRDs under `crds/`)
