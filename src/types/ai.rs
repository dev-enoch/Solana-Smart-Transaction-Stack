use serde::{Deserialize, Serialize};

/// Context provided to the AI agent when a failure occurs.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Snapshot of current network conditions for the AI agent to consider.
    pub network_snapshot: Option<NetworkSnapshot>,
}

/// A snapshot of network conditions at the time of failure,
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkSnapshot {
    pub avg_recent_priority_fee: Option<u64>,
    pub p75_recent_priority_fee: Option<u64>,
    pub avg_tip_account_balance: Option<u64>,
    pub current_dynamic_tip: Option<u64>,
    pub slots_since_last_jito_leader: Option<u64>,
    pub recent_landing_rate_pct: Option<f64>,
}

/// The decision produced by the AI agent after reasoning about a failure.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentDecision {
    pub reasoning: String,
    pub root_cause: String,
    pub action: String,
    pub new_tip_lamports: Option<u64>,
    pub wait_slots: Option<u64>,
    /// Agent's confidence in this decision (0.0 to 1.0).
    #[serde(default)]
    pub confidence: Option<f64>,
}

/// Configuration for the AI agent's LLM provider(s).
#[derive(Debug, Clone)]
pub struct AiConfig {
    pub primary_api_url: String,
    pub primary_api_key: String,
    pub primary_model: String,
    /// Optional fallback provider for resilience.
    pub fallback_api_url: Option<String>,
    pub fallback_api_key: Option<String>,
    pub fallback_model: Option<String>,
}

/// Identifies which LLM provider format to use.
#[derive(Debug, Clone, PartialEq)]
pub enum LlmProvider {
    /// Google Generative AI (Gemini) — uses `contents` format
    Gemini,
    /// OpenAI-compatible (OpenAI, xAI/Grok, Groq, etc.) — uses `messages` format
    OpenAiCompatible,
}

impl LlmProvider {
    /// Auto-detect provider from the API URL.
    pub fn from_url(url: &str) -> Self {
        if url.contains("googleapis.com") || url.contains("generativelanguage") {
            Self::Gemini
        } else {
            Self::OpenAiCompatible
        }
    }
}
