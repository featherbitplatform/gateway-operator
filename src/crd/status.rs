//! Status types and condition helpers shared by every kind.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const ACCEPTED: &str = "Accepted";
pub const RESOLVED_REFS: &str = "ResolvedRefs";
pub const PROGRAMMED: &str = "Programmed";
pub const READY: &str = "Ready";

pub const REASON_VALID: &str = "Valid";
pub const REASON_INVALID: &str = "Invalid";
pub const REASON_CONFLICTED: &str = "Conflicted";
pub const REASON_COMPILE_FAILED: &str = "CompileFailed";
pub const REASON_RESOLVED: &str = "Resolved";
pub const REASON_POLICY_NOT_FOUND: &str = "PolicyNotFound";
pub const REASON_PLUGIN_CONFIG_NOT_FOUND: &str = "PluginConfigNotFound";
pub const REASON_SUPERNODE_NOT_FOUND: &str = "SupernodeNotFound";
pub const REASON_PROGRAMMED: &str = "Programmed";
pub const REASON_NOT_SELECTED: &str = "NotSelected";
pub const REASON_EXCLUDED: &str = "Excluded";
pub const REASON_GATEWAY_NOT_READY: &str = "GatewayNotReady";
pub const REASON_READY: &str = "Rendered";
pub const REASON_INVALID_SPEC: &str = "InvalidSpec";
pub const REASON_SINK_UNAVAILABLE: &str = "SinkUnavailable";

/// Status of the six resource kinds.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceStatus {
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Gateways whose rendered config includes this object.
    #[serde(default)]
    pub gateways: Vec<GatewayRef>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
pub struct GatewayRef {
    pub namespace: String,
    pub name: String,
}

/// Builds a condition. `status` true gives "True".
pub fn cond(type_: &str, status: bool, reason: &str, message: &str, generation: i64) -> Condition {
    Condition {
        type_: type_.to_string(),
        status: if status { "True" } else { "False" }.to_string(),
        reason: reason.to_string(),
        message: message.to_string(),
        observed_generation: Some(generation),
        last_transition_time: Time(k8s_openapi::jiff::Timestamp::now()),
    }
}

/// Replaces the condition of the same type, keeping `lastTransitionTime` when
/// the status value did not change (Kubernetes convention).
pub fn set_condition(conditions: &mut Vec<Condition>, mut new: Condition) {
    if let Some(existing) = conditions.iter_mut().find(|c| c.type_ == new.type_) {
        if existing.status == new.status {
            new.last_transition_time = existing.last_transition_time.clone();
        }
        *existing = new;
    } else {
        conditions.push(new);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_condition_replaces_same_type_and_keeps_transition_time_when_status_unchanged() {
        let mut conds = vec![cond(ACCEPTED, true, REASON_VALID, "ok", 1)];
        let t0 = conds[0].last_transition_time.clone();
        set_condition(
            &mut conds,
            cond(ACCEPTED, true, REASON_VALID, "still ok", 2),
        );
        assert_eq!(conds.len(), 1);
        assert_eq!(conds[0].message, "still ok");
        assert_eq!(conds[0].observed_generation, Some(2));
        assert_eq!(conds[0].last_transition_time, t0);
        set_condition(&mut conds, cond(ACCEPTED, false, REASON_INVALID, "bad", 3));
        assert_eq!(conds[0].status, "False");
        set_condition(&mut conds, cond(PROGRAMMED, true, REASON_PROGRAMMED, "", 3));
        assert_eq!(conds.len(), 2);
    }
}
