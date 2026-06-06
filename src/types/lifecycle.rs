use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleEntry {
    pub bundle_id: String,
    pub slot_submitted: u64,
    pub submitted_at: DateTime<Utc>,
    
    pub processed_at: Option<DateTime<Utc>>,
    pub processed_slot: Option<u64>,
    
    pub confirmed_at: Option<DateTime<Utc>>,
    pub confirmed_slot: Option<u64>,
    
    pub finalized_at: Option<DateTime<Utc>>,
    pub finalized_slot: Option<u64>,
    
    pub tip_lamports: u64,
    pub status: String,           // "success" | "failed"
    pub failure_type: Option<String>, 
    pub latency_processed_ms: Option<i64>,
    pub latency_confirmed_ms: Option<i64>,
    pub latency_finalized_ms: Option<i64>,
    pub last_valid_block_height: Option<u64>,
    pub retry_count: u32,
    pub signatures: Vec<String>,
}

impl Default for LifecycleEntry {
    fn default() -> Self {
        Self {
            bundle_id: String::new(),
            slot_submitted: 0,
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
            last_valid_block_height: None,
            retry_count: 0,
            signatures: Vec::new(),
        }
    }
}
