use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::types::lifecycle::LifecycleEntry;

#[derive(Clone)]
pub struct LifecycleTracker {
    entries: Arc<RwLock<HashMap<String, LifecycleEntry>>>,
    sig_to_bundle: Arc<RwLock<HashMap<String, String>>>,
    log_file: String,
}

impl LifecycleTracker {
    pub fn new(log_file: &str) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            sig_to_bundle: Arc::new(RwLock::new(HashMap::new())),
            log_file: log_file.to_string(),
        }
    }

    pub async fn record_submission(&self, bundle_id: String, slot: u64, tip: u64, signatures: Vec<String>, last_valid_block_height: u64) {
        let entry = LifecycleEntry {
            bundle_id: bundle_id.clone(),
            slot_submitted: slot,
            submitted_at: Utc::now(),
            tip_lamports: tip,
            last_valid_block_height: Some(last_valid_block_height),
            signatures: signatures.clone(),
            ..Default::default()
        };
        self.entries.write().await.insert(bundle_id.clone(), entry);
        
        let mut sig_map = self.sig_to_bundle.write().await;
        for sig in signatures {
            sig_map.insert(sig, bundle_id.clone());
        }
    }

    pub async fn update_status(&self, bundle_id: &str, commitment: &str, slot: u64) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(bundle_id) {
            let now = Utc::now();
            match commitment {
                "processed" => {
                    entry.processed_at = Some(now);
                    entry.processed_slot = Some(slot);
                    entry.latency_processed_ms = Some((now - entry.submitted_at).num_milliseconds());
                    entry.status = "processed".to_string();
                }
                "confirmed" => { 
                    entry.confirmed_at = Some(now);
                    entry.confirmed_slot = Some(slot);
                    entry.latency_confirmed_ms = Some((now - entry.submitted_at).num_milliseconds());
                    entry.status = "confirmed".to_string();
                }
                "finalized" => { 
                    entry.finalized_at = Some(now);
                    entry.finalized_slot = Some(slot);
                    entry.status = "finalized".to_string();
                }
                _ => {}
            }
            info!("Updated commitment for {} to {}", bundle_id, commitment);
        }
    }

    pub async fn update_status_by_sig(&self, signature: &str, commitment: &str, slot: u64) {
        let bundle_id = {
            let map = self.sig_to_bundle.read().await;
            map.get(signature).cloned()
        };
        if let Some(bid) = bundle_id {
            self.update_status(&bid, commitment, slot).await;
        }
    }

    pub async fn record_failure(&self, bundle_id: &str, failure_type: String) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(bundle_id) {
            entry.status = "failed".to_string();
            entry.failure_type = Some(failure_type.clone());
            warn!("Recorded failure for {}: {}", bundle_id, failure_type);
        }
    }

    pub async fn check_expiries(&self, current_slot: u64) -> Vec<(String, u64, u64)> {
        // Returns a list of (bundle_id, slot_submitted, tip_lamports) that just expired
        let mut expired = Vec::new();
        let mut entries = self.entries.write().await;
        for (bid, entry) in entries.iter_mut() {
            if entry.status == "pending" {
                if let Some(lvbh) = entry.last_valid_block_height {
                    if current_slot > lvbh {
                        entry.status = "failed".to_string();
                        entry.failure_type = Some("expired_blockhash".to_string());
                        warn!("Detected blockhash expiry for {}", bid);
                        expired.push((bid.clone(), entry.slot_submitted, entry.tip_lamports));
                    }
                }
            }
        }
        expired
    }

    pub async fn save_logs(&self) -> Result<()> {
        let entries = self.entries.read().await;
        let values: Vec<_> = entries.values().collect();
        let json = serde_json::to_string_pretty(&values)?;
        tokio::fs::write(&self.log_file, json).await?;
        info!("Lifecycle logs saved to {}", self.log_file);
        Ok(())
    }
}
