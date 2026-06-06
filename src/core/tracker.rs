use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use solana_transaction_status::TransactionConfirmationStatus;
use std::str::FromStr;

use crate::types::lifecycle::LifecycleEntry;
use crate::logging::{StructuredLogger, OperationalEvent};

/// Tracks the full lifecycle of submitted bundles across all Solana commitment levels.
#[derive(Clone)]
pub struct LifecycleTracker {
    entries: Arc<RwLock<HashMap<String, LifecycleEntry>>>,
    sig_to_bundle: Arc<RwLock<HashMap<String, String>>>,
    log_file: String,
    rpc_client: Arc<RpcClient>,
    logger: StructuredLogger,
}

impl LifecycleTracker {
    pub fn new(log_file: &str, rpc_url: &str, logger: StructuredLogger) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            sig_to_bundle: Arc::new(RwLock::new(HashMap::new())),
            log_file: log_file.to_string(),
            rpc_client: Arc::new(RpcClient::new(rpc_url.to_string())),
            logger,
        }
    }

    /// Record a new bundle submission in the tracker.
    pub async fn record_submission(
        &self,
        bundle_id: String,
        slot: u64,
        tip: u64,
        signatures: Vec<String>,
        last_valid_block_height: u64,
    ) {
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

    /// Ordering for commitment levels — used to prevent status downgrades.
    fn commitment_ord(s: &str) -> u8 {
        match s {
            "pending" => 0,
            "processed" => 1,
            "confirmed" => 2,
            "finalized" => 3,
            _ => 0,
        }
    }

    /// Update the commitment status of a bundle.
    pub async fn update_status(&self, bundle_id: &str, commitment: &str, slot: u64) {
        let event = {
            let mut entries = self.entries.write().await;
            let entry = match entries.get_mut(bundle_id) {
                Some(e) => e,
                None => return,
            };

            // Don't update already-failed entries with commitment updates
            if entry.status == "failed" && commitment != "failed" {
                return;
            }

            let now = Utc::now();

            match commitment {
                "processed" if entry.processed_at.is_none() => {
                    entry.processed_at = Some(now);
                    entry.processed_slot = Some(slot);
                    let lat = (now - entry.submitted_at).num_milliseconds();
                    entry.latency_processed_ms = Some(lat);
                    if Self::commitment_ord("processed") > Self::commitment_ord(&entry.status) {
                        entry.status = "processed".to_string();
                    }
                    Some(OperationalEvent::CommitmentUpdate {
                        timestamp: now,
                        bundle_id: bundle_id.to_string(),
                        commitment: "processed".to_string(),
                        slot,
                        latency_ms: Some(lat),
                    })
                }
                "confirmed" if entry.confirmed_at.is_none() => {
                    entry.confirmed_at = Some(now);
                    entry.confirmed_slot = Some(slot);
                    let lat = (now - entry.submitted_at).num_milliseconds();
                    entry.latency_confirmed_ms = Some(lat);
                    if Self::commitment_ord("confirmed") > Self::commitment_ord(&entry.status) {
                        entry.status = "confirmed".to_string();
                    }
                    Some(OperationalEvent::CommitmentUpdate {
                        timestamp: now,
                        bundle_id: bundle_id.to_string(),
                        commitment: "confirmed".to_string(),
                        slot,
                        latency_ms: Some(lat),
                    })
                }
                "finalized" if entry.finalized_at.is_none() => {
                    entry.finalized_at = Some(now);
                    entry.finalized_slot = Some(slot);
                    if Self::commitment_ord("finalized") > Self::commitment_ord(&entry.status) {
                        entry.status = "finalized".to_string();
                    }
                    Some(OperationalEvent::CommitmentUpdate {
                        timestamp: now,
                        bundle_id: bundle_id.to_string(),
                        commitment: "finalized".to_string(),
                        slot,
                        latency_ms: None,
                    })
                }
                "failed" if entry.status != "failed" => {
                    entry.status = "failed".to_string();
                    Some(OperationalEvent::FailureDetected {
                        timestamp: now,
                        bundle_id: bundle_id.to_string(),
                        failure_type: "transaction_error".to_string(),
                        slot,
                        details: "Transaction error observed via stream".to_string(),
                    })
                }
                _ => None,
            }
        }; // Write lock dropped here

        if let Some(ref evt) = event {
            self.logger.log(evt).await;
            match evt {
                OperationalEvent::CommitmentUpdate { bundle_id, commitment, slot, .. } => {
                    info!("Commitment: {} → {} at slot {}", bundle_id, commitment, slot);
                }
                OperationalEvent::FailureDetected { bundle_id, failure_type, .. } => {
                    warn!("Failure: {} — {}", bundle_id, failure_type);
                }
                _ => {}
            }
        }
    }

    /// Resolve a transaction signature to its bundle ID and update status.
    pub async fn update_status_by_sig(&self, signature: &str, commitment: &str, slot: u64) {
        let bundle_id = {
            let map = self.sig_to_bundle.read().await;
            map.get(signature).cloned()
        };
        if let Some(bid) = bundle_id {
            self.update_status(&bid, commitment, slot).await;
        }
    }

    /// Record a failure for a specific bundle.
    pub async fn record_failure(&self, bundle_id: &str, failure_type: String) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(bundle_id) {
            entry.status = "failed".to_string();
            entry.failure_type = Some(failure_type.clone());
            warn!("Recorded failure for {}: {}", bundle_id, failure_type);
        }
    }

    /// Check for bundles whose blockhash has expired.
    pub async fn check_expiries(&self, current_slot: u64) -> Vec<(String, u64, u64)> {
        let mut expired = Vec::new();
        let mut entries = self.entries.write().await;
        for (bid, entry) in entries.iter_mut() {
            if entry.status == "pending" {
                if let Some(lvbh) = entry.last_valid_block_height {
                    if current_slot > lvbh {
                        entry.status = "failed".to_string();
                        entry.failure_type = Some("expired_blockhash".to_string());
                        warn!("Blockhash expiry detected for bundle {}", bid);
                        expired.push((bid.clone(), entry.slot_submitted, entry.tip_lamports));
                    }
                }
            }
        }
        expired
    }

    /// Start a background task that polls RPC for commitment status updates.
    pub fn start_commitment_poller(&self) {
        let entries = self.entries.clone();
        let rpc_client = self.rpc_client.clone();
        let logger = self.logger.clone();

        info!("Commitment poller started (3s interval, polling confirmed + finalized)");

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
            loop {
                interval.tick().await;

                // Collect signatures that need commitment polling
                let sigs_to_check: Vec<(String, String)> = {
                    let entries_map = entries.read().await;
                    let mut result = Vec::new();
                    for (bid, entry) in entries_map.iter() {
                        // Only poll for entries that haven't reached finalized or failed
                        if entry.status != "finalized" && entry.status != "failed" {
                            for sig in &entry.signatures {
                                result.push((sig.clone(), bid.clone()));
                            }
                        }
                    }
                    result
                };

                if sigs_to_check.is_empty() {
                    continue;
                }

                // Parse signature strings to Signature type
                let parsed_sigs: Vec<(Signature, String)> = sigs_to_check
                    .iter()
                    .filter_map(|(sig_str, bid)| {
                        Signature::from_str(sig_str).ok().map(|s| (s, bid.clone()))
                    })
                    .collect();

                if parsed_sigs.is_empty() {
                    continue;
                }

                let sig_refs: Vec<Signature> = parsed_sigs.iter().map(|(s, _)| *s).collect();

                // Query signature statuses from RPC
                match rpc_client.get_signature_statuses(&sig_refs).await {
                    Ok(response) => {
                        // Collect updates while holding the write lock briefly
                        let updates: Vec<(String, String, u64)> = {
                            let mut entries_map = entries.write().await;
                            let mut updates = Vec::new();

                            for (i, status_opt) in response.value.iter().enumerate() {
                                if let Some(status) = status_opt {
                                    let bid = &parsed_sigs[i].1;

                                    if let Some(ref confirmation_status) =
                                        status.confirmation_status
                                    {
                                        let commitment_str = match confirmation_status {
                                            TransactionConfirmationStatus::Processed => "processed",
                                            TransactionConfirmationStatus::Confirmed => "confirmed",
                                            TransactionConfirmationStatus::Finalized => "finalized",
                                        };

                                        if let Some(entry) = entries_map.get_mut(bid.as_str()) {
                                            let should_update = match
                                                (entry.status.as_str(), commitment_str)
                                            {
                                                ("pending", _) => true,
                                                ("processed", "confirmed" | "finalized") => true,
                                                ("confirmed", "finalized") => true,
                                                _ => false,
                                            };

                                            if should_update {
                                                let now = Utc::now();
                                                let mut updated = false;

                                                match commitment_str {
                                                    "processed"
                                                        if entry.processed_at.is_none() =>
                                                    {
                                                        entry.processed_at = Some(now);
                                                        entry.processed_slot = Some(status.slot);
                                                        entry.latency_processed_ms = Some(
                                                            (now - entry.submitted_at)
                                                                .num_milliseconds(),
                                                        );
                                                        updated = true;
                                                    }
                                                    "confirmed"
                                                        if entry.confirmed_at.is_none() =>
                                                    {
                                                        entry.confirmed_at = Some(now);
                                                        entry.confirmed_slot = Some(status.slot);
                                                        entry.latency_confirmed_ms = Some(
                                                            (now - entry.submitted_at)
                                                                .num_milliseconds(),
                                                        );
                                                        updated = true;
                                                    }
                                                    "finalized"
                                                        if entry.finalized_at.is_none() =>
                                                    {
                                                        entry.finalized_at = Some(now);
                                                        entry.finalized_slot = Some(status.slot);
                                                        updated = true;
                                                    }
                                                    _ => {}
                                                }

                                                if updated {
                                                    // Only advance status forward
                                                    let new_ord = match commitment_str {
                                                        "processed" => 1u8,
                                                        "confirmed" => 2,
                                                        "finalized" => 3,
                                                        _ => 0,
                                                    };
                                                    let cur_ord = match entry.status.as_str() {
                                                        "pending" => 0u8,
                                                        "processed" => 1,
                                                        "confirmed" => 2,
                                                        "finalized" => 3,
                                                        _ => 0,
                                                    };
                                                    if new_ord > cur_ord {
                                                        entry.status =
                                                            commitment_str.to_string();
                                                    }

                                                    updates.push((
                                                        bid.clone(),
                                                        commitment_str.to_string(),
                                                        status.slot,
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            updates
                        }; // Write lock dropped here

                        // Log updates outside the lock
                        for (bid, commitment, slot) in &updates {
                            let now = Utc::now();
                            let lat = {
                                let e = entries.read().await;
                                e.get(bid.as_str()).and_then(|entry| {
                                    match commitment.as_str() {
                                        "processed" => entry.latency_processed_ms,
                                        "confirmed" => entry.latency_confirmed_ms,
                                        _ => None,
                                    }
                                })
                            };

                            logger
                                .log(&OperationalEvent::CommitmentUpdate {
                                    timestamp: now,
                                    bundle_id: bid.clone(),
                                    commitment: commitment.clone(),
                                    slot: *slot,
                                    latency_ms: lat,
                                })
                                .await;

                            info!("[Poller] {} → {} (slot {})", bid, commitment, slot);
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Commitment poll error: {}", e);
                    }
                }
            }
        });
    }

    /// Persist all lifecycle entries to the JSON log file.
    pub async fn save_logs(&self) -> Result<()> {
        let entries = self.entries.read().await;
        let values: Vec<_> = entries.values().collect();
        let json = serde_json::to_string_pretty(&values)?;
        tokio::fs::write(&self.log_file, json).await?;
        info!("Lifecycle logs saved ({} entries)", values.len());
        Ok(())
    }
}
