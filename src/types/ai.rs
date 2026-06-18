use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FailureContext {
    pub intent_id: String,
    pub bundle_id: String,
    pub failure_type: String,
    pub slot: u64,
    pub tip: u64,
    pub latency: i64,
    pub extra: String,
    pub retry_count: u32,
    pub history_summary: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AgentDecision {
    pub reasoning: String,
    pub root_cause: String,
    pub action: String,
    pub new_tip_lamports: Option<u64>,
    pub wait_slots: Option<u64>,
}
