use chrono::{DateTime, Utc};
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Structured operational events for the JSONL audit log.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event")]
pub enum OperationalEvent {
    /// System startup with configuration summary.
    #[serde(rename = "system_startup")]
    SystemStartup {
        timestamp: DateTime<Utc>,
        network: String,
        rpc_url: String,
        jito_url: String,
        yellowstone_endpoint: String,
        ai_primary_provider: String,
        ai_fallback_provider: Option<String>,
        payer_pubkey: String,
        jito_validator_count: usize,
    },

    /// A bundle was successfully submitted to the Jito Block Engine.
    #[serde(rename = "bundle_submitted")]
    BundleSubmitted {
        timestamp: DateTime<Utc>,
        bundle_id: String,
        slot: u64,
        block_height: Option<u64>,
        tip_lamports: u64,
        signatures: Vec<String>,
        memo: String,
        fault_injection: Option<String>,
    },

    /// A transaction commitment level was observed (processed/confirmed/finalized).
    #[serde(rename = "commitment_update")]
    CommitmentUpdate {
        timestamp: DateTime<Utc>,
        bundle_id: String,
        commitment: String,
        slot: u64,
        latency_ms: Option<i64>,
    },

    /// A failure was detected (blockhash expiry, transaction error, etc.).
    #[serde(rename = "failure_detected")]
    FailureDetected {
        timestamp: DateTime<Utc>,
        bundle_id: String,
        failure_type: String,
        slot: u64,
        details: String,
    },

    /// The AI agent made an autonomous decision about a failure.
    #[serde(rename = "ai_decision")]
    AiDecision {
        timestamp: DateTime<Utc>,
        bundle_id: String,
        action: String,
        reasoning: String,
        root_cause: String,
        new_tip: Option<u64>,
        wait_slots: Option<u64>,
        confidence: Option<f64>,
        provider: String,
    },

    /// A dynamic tip was calculated from network data.
    #[serde(rename = "tip_calculated")]
    TipCalculated {
        timestamp: DateTime<Utc>,
        base_lamports: u64,
        congestion_factor: f64,
        result_lamports: u64,
        recent_fee_count: usize,
        p75_fee: Option<u64>,
        avg_tip_account_balance_lamports: Option<u64>,
    },

    /// A retry intent was queued by the AI agent.
    #[serde(rename = "retry_queued")]
    RetryQueued {
        timestamp: DateTime<Utc>,
        original_bundle_id: String,
        new_intent_id: String,
        reason: String,
        new_tip: Option<u64>,
    },

    /// The Yellowstone gRPC stream connected successfully.
    #[serde(rename = "stream_connected")]
    StreamConnected {
        timestamp: DateTime<Utc>,
        endpoint: String,
    },

    /// The Yellowstone gRPC stream disconnected.
    #[serde(rename = "stream_disconnected")]
    StreamDisconnected {
        timestamp: DateTime<Utc>,
        endpoint: String,
        backoff_secs: u64,
        reconnection_count: u64,
    },

    /// A slot was observed from the stream (sampled — not every slot is logged).
    #[serde(rename = "slot_observed")]
    SlotObserved {
        timestamp: DateTime<Utc>,
        slot: u64,
        block_height: Option<u64>,
        leader: Option<String>,
        is_jito_window: bool,
    },

    /// A bundle submission attempt failed at the HTTP/RPC level.
    #[serde(rename = "submission_error")]
    SubmissionError {
        timestamp: DateTime<Utc>,
        intent_id: String,
        error: String,
        slot: u64,
    },

    /// Bundle status retrieved from Jito's getBundleStatuses API.
    #[serde(rename = "bundle_status_update")]
    BundleStatusUpdate {
        timestamp: DateTime<Utc>,
        bundle_id: String,
        status: String,
        landed_slot: Option<u64>,
    },

    /// Stream health metrics report (emitted periodically).
    #[serde(rename = "stream_health")]
    StreamHealth {
        timestamp: DateTime<Utc>,
        messages_per_sec: f64,
        total_slot_updates: u64,
        total_tx_updates: u64,
        messages_dropped: u64,
        reconnection_count: u64,
    },
}

/// Append-only structured JSON logger for operational events.
#[derive(Clone)]
pub struct StructuredLogger {
    file: Arc<Mutex<File>>,
}

impl StructuredLogger {
    /// Create a new structured logger that appends to the given file path.
    pub fn new(path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    /// Log a single operational event as a JSON line.
    pub async fn log(&self, event: &OperationalEvent) {
        match serde_json::to_string(event) {
            Ok(json) => {
                let mut file = self.file.lock().await;
                if let Err(e) = writeln!(file, "{}", json) {
                    tracing::error!("Failed to write structured log: {}", e);
                }
                let _ = file.flush();
            }
            Err(e) => {
                tracing::error!("Failed to serialize operational event: {}", e);
            }
        }
    }
}
