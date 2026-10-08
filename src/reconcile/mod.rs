//! The reconcile pipeline. `select`, `verdict`, `render` and `plan` are pure
//! and unit-tested; `status`, the sinks and the controller in this file do IO.
pub mod plan;
pub mod render;
pub mod select;
pub mod status;
pub mod verdict;
