use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::types::lifecycle::LifecycleEntry;
use crate::logging::{StructuredLogger, OperationalEvent};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use std::str::FromStr;

/// Maximum number of retries before a bundle chain is abandoned.
pub const MAX_RETRIES: u32 = 3;

/// Tracks the full lifecycle of submitted bundles across all Solana commitment levels.
#[derive(Clone)]
pub struct LifecycleTracker {
    entries: Arc<RwLock<HashMap<String, LifecycleEntry>>>,
    sig_to_bundle: Arc<RwLock<HashMap<String, String>>>,
    log_file: String,
    logger: StructuredLogger,
    rpc_client: Arc<RpcClient>,
}

impl LifecycleTracker {
    pub fn new(log_file: &str, logger: StructuredLogger, rpc_client: Arc<RpcClient>) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            sig_to_bundle: Arc::new(RwLock::new(HashMap::new())),
            log_file: log_file.to_string(),
            logger,
            rpc_client,
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
        retry_count: u32,
    ) {
        let entry = LifecycleEntry {
            bundle_id: bundle_id.clone(),
            slot_submitted: slot,
            submitted_at: Utc::now(),
            tip_lamports: tip,
            last_valid_block_height: Some(last_valid_block_height),
            signatures: signatures.clone(),
            retry_count,
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

    /// Classify a transaction error string into a failure category.
    pub fn classify_failure(error: &str) -> String {
        let lower = error.to_lowercase();
        if lower.contains("blockhashnotfound") || lower.contains("blockhash not found") {
            "expired_blockhash".to_string()
        } else if lower.contains("insufficientfunds") || lower.contains("insufficient funds")
            || lower.contains("insufficient lamports")
        {
            "insufficient_funds".to_string()
        } else if lower.contains("computationalbudgetexceeded")
            || lower.contains("computational budget exceeded")
            || lower.contains("exceeded CUs meter")
        {
            "compute_exceeded".to_string()
        } else if lower.contains("alreadyprocessed") || lower.contains("already processed") {
            "already_processed".to_string()
        } else if lower.contains("bundle") {
            "bundle_failure".to_string()
        } else {
            "transaction_error".to_string()
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
                    let lat = (now - entry.submitted_at).num_milliseconds();
                    entry.latency_finalized_ms = Some(lat);
                    if Self::commitment_ord("finalized") > Self::commitment_ord(&entry.status) {
                        entry.status = "finalized".to_string();
                    }
                    Some(OperationalEvent::CommitmentUpdate {
                        timestamp: now,
                        bundle_id: bundle_id.to_string(),
                        commitment: "finalized".to_string(),
                        slot,
                        latency_ms: Some(lat),
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

    /// Record a failure with classification for a specific bundle.
    pub async fn update_status_failed(&self, bundle_id: &str, error_msg: &str, slot: u64) {
        let failure_type = Self::classify_failure(error_msg);
        let event = {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.get_mut(bundle_id) {
                if entry.status != "failed" {
                    entry.status = "failed".to_string();
                    entry.failure_type = Some(failure_type.clone());
                    Some(OperationalEvent::FailureDetected {
                        timestamp: Utc::now(),
                        bundle_id: bundle_id.to_string(),
                        failure_type: failure_type.clone(),
                        slot,
                        details: error_msg.to_string(),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(ref evt) = event {
            self.logger.log(evt).await;
            warn!("Classified failure for {}: {}", bundle_id, error_msg);
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

    /// Resolve a transaction signature to its bundle ID and update with classified failure.
    pub async fn update_failure_by_sig(&self, signature: &str, error_msg: &str, slot: u64) {
        let bundle_id = {
            let map = self.sig_to_bundle.read().await;
            map.get(signature).cloned()
        };
        if let Some(bid) = bundle_id {
            self.update_status_failed(&bid, error_msg, slot).await;
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
    /// Returns (bundle_id, slot_submitted, tip_lamports, submitted_at_ms, retry_count).
    pub async fn check_expiries(&self, current_slot: u64) -> Vec<(String, u64, u64, i64, u32)> {
        let mut expired = Vec::new();
        let mut entries = self.entries.write().await;
        for (bid, entry) in entries.iter_mut() {
            if entry.status == "pending" {
                if let Some(lvbh) = entry.last_valid_block_height {
                    if current_slot > lvbh {
                        entry.status = "failed".to_string();
                        entry.failure_type = Some("expired_blockhash".to_string());
                        let age_ms = (Utc::now() - entry.submitted_at).num_milliseconds();
                        warn!("Blockhash expiry detected for bundle {}", bid);
                        expired.push((
                            bid.clone(),
                            entry.slot_submitted,
                            entry.tip_lamports,
                            age_ms,
                            entry.retry_count,
                        ));
                    }
                }
            }
        }
        expired
    }

    /// Advance commitment levels for tracked bundles based on stream slot updates.
    pub async fn advance_commitments_by_slot(&self, current_slot: u64, status: i32) {
        let target_commitment = match status {
            1 => "confirmed",
            2 => "finalized",
            _ => return,
        };

        let updates: Vec<(String, String, u64, Option<i64>)> = {
            let mut entries = self.entries.write().await;
            let mut updates = Vec::new();

            for (bid, entry) in entries.iter_mut() {
                // Skip already-failed or already-finalized entries
                if entry.status == "failed" || entry.status == "finalized" {
                    continue;
                }

                // Only advance bundles that have been processed at or before this slot
                let processed_slot = match entry.processed_slot {
                    Some(s) => s,
                    None => continue,
                };

                if processed_slot > current_slot {
                    continue;
                }

                let now = Utc::now();

                match target_commitment {
                    "confirmed" if entry.confirmed_at.is_none() => {
                        entry.confirmed_at = Some(now);
                        entry.confirmed_slot = Some(current_slot);
                        let lat = (now - entry.submitted_at).num_milliseconds();
                        entry.latency_confirmed_ms = Some(lat);
                        if Self::commitment_ord("confirmed") > Self::commitment_ord(&entry.status) {
                            entry.status = "confirmed".to_string();
                        }
                        updates.push((bid.clone(), "confirmed".to_string(), current_slot, Some(lat)));
                    }
                    "finalized" => {
                        // On finalized, also backfill confirmed if missing
                        if entry.confirmed_at.is_none() {
                            entry.confirmed_at = Some(now);
                            entry.confirmed_slot = Some(current_slot);
                            let lat = (now - entry.submitted_at).num_milliseconds();
                            entry.latency_confirmed_ms = Some(lat);
                            if Self::commitment_ord("confirmed") > Self::commitment_ord(&entry.status) {
                                entry.status = "confirmed".to_string();
                            }
                            updates.push((bid.clone(), "confirmed".to_string(), current_slot, Some(lat)));
                        }

                        if entry.finalized_at.is_none() {
                            entry.finalized_at = Some(now);
                            entry.finalized_slot = Some(current_slot);
                            let lat = (now - entry.submitted_at).num_milliseconds();
                            entry.latency_finalized_ms = Some(lat);
                            if Self::commitment_ord("finalized") > Self::commitment_ord(&entry.status) {
                                entry.status = "finalized".to_string();
                            }
                            updates.push((bid.clone(), "finalized".to_string(), current_slot, Some(lat)));
                        }
                    }
                    _ => {}
                }
            }

            updates
        }; // Write lock dropped here

        // Log updates outside the lock
        for (bid, commitment, slot, lat) in &updates {
            self.logger
                .log(&OperationalEvent::CommitmentUpdate {
                    timestamp: Utc::now(),
                    bundle_id: bid.clone(),
                    commitment: commitment.clone(),
                    slot: *slot,
                    latency_ms: *lat,
                })
                .await;

            info!("[Stream] {} → {} (slot {})", bid, commitment, slot);
        }
    }

    /// Poll `getSignatureStatuses` for pending/processed bundles as secondary confirmation.
    /// This supplements the gRPC stream with RPC-based verification.
    pub async fn poll_signature_statuses(&self) {
        // Collect signatures that still need confirmation
        let sigs_to_check: Vec<(String, String)> = {
            let entries = self.entries.read().await;
            entries
                .values()
                .filter(|e| e.status == "processed" || e.status == "confirmed")
                .flat_map(|e| {
                    e.signatures
                        .iter()
                        .map(|s| (s.clone(), e.bundle_id.clone()))
                        .collect::<Vec<_>>()
                })
                .collect()
        };

        if sigs_to_check.is_empty() {
            return;
        }

        // Parse signatures
        let parsed: Vec<(Signature, String)> = sigs_to_check
            .iter()
            .filter_map(|(sig_str, bid)| {
                Signature::from_str(sig_str)
                    .ok()
                    .map(|sig| (sig, bid.clone()))
            })
            .collect();

        if parsed.is_empty() {
            return;
        }

        let sig_refs: Vec<Signature> = parsed.iter().map(|(s, _)| *s).collect();

        // Query confirmed status
        match self.rpc_client.get_signature_statuses(&sig_refs).await {
            Ok(response) => {
                for (i, status_opt) in response.value.iter().enumerate() {
                    if let Some(status) = status_opt {
                        let bid = &parsed[i].1;
                        if let Some(ref err) = status.err {
                            self.update_status_failed(bid, &format!("{:?}", err), status.slot)
                                .await;
                        } else if let Some(ref confirmation) = status.confirmation_status {
                            let commitment_str = format!("{:?}", confirmation).to_lowercase();
                            self.update_status(bid, &commitment_str, status.slot).await;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!("Signature status poll failed: {:?}", e);
            }
        }
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
