//! featherbit gateway-operator: config-as-CRDs for the featherbit API gateway.
//!
//! Modules are added by the tasks that fill them; keep this list in sync.
pub mod crd;
pub mod reconcile;
pub mod sink;
pub mod telemetry;
pub mod validators;
pub mod webhook;
