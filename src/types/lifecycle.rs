use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Full lifecycle record for a single bundle submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleEntry {
    pub intent_id: String,
    pub bundle_id: String,

    // ── Submission ───────────────────────────────────────────────
    pub slot_submitted: u64,
    pub block_height_submitted: Option<u64>,
    pub submitted_at: DateTime<Utc>,

    // ── Processed (execution confirmed by validator) ─────────────
    pub processed_at: Option<DateTime<Utc>>,
    pub processed_slot: Option<u64>,

    // ── Confirmed (supermajority vote) ───────────────────────────
    pub confirmed_at: Option<DateTime<Utc>>,
    pub confirmed_slot: Option<u64>,

    // ── Finalized (rooted, irreversible) ─────────────────────────
    pub finalized_at: Option<DateTime<Utc>>,
    pub finalized_slot: Option<u64>,

    // ── Tip & status ─────────────────────────────────────────────
    pub tip_lamports: u64,
    /// Current status: "pending" | "processed" | "confirmed" | "finalized" | "failed"
    pub status: String,
    pub failure_type: Option<String>,

    // ── Latency deltas (milliseconds) ────────────────────────────
    pub latency_processed_ms: Option<i64>,
    /// submitted → confirmed
    pub latency_confirmed_ms: Option<i64>,
    /// submitted → finalized
    pub latency_finalized_ms: Option<i64>,
    /// processed → confirmed (network health indicator)
    pub latency_processed_to_confirmed_ms: Option<i64>,

    // ── Expiry & retry tracking ──────────────────────────────────
    pub last_valid_block_height: Option<u64>,
    pub retry_count: u32,
    pub signatures: Vec<String>,
    pub history_summary: String,

    // ── AI decision audit trail ──────────────────────────────────
    #[serde(default)]
    pub ai_decisions: Vec<AiDecisionRecord>,
}

/// A single recorded AI decision, stored on the lifecycle entry for audit purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiDecisionRecord {
    pub timestamp: DateTime<Utc>,
    pub action: String,
    pub reasoning: String,
    pub root_cause: String,
    pub new_tip: Option<u64>,
    pub confidence: Option<f64>,
}

impl Default for LifecycleEntry {
    fn default() -> Self {
        Self {
            intent_id: String::new(),
            bundle_id: String::new(),
            slot_submitted: 0,
            block_height_submitted: None,
            submitted_at: Utc::now(),
            processed_at: None,
            processed_slot: None,
            confirmed_at: None,
            confirmed_slot: None,
            finalized_at: None,
            finalized_slot: None,
            tip_lamports: 0,
            status: "pending".to_string(),
            failure_type: None,
            latency_processed_ms: None,
            latency_confirmed_ms: None,
            latency_finalized_ms: None,
            latency_processed_to_confirmed_ms: None,
            last_valid_block_height: None,
            retry_count: 0,
            signatures: Vec::new(),
            history_summary: String::new(),
            ai_decisions: Vec::new(),
        }
    }
}
