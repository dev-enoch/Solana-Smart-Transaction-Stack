use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};
use colored::*;

use crate::logging::{OperationalEvent, StructuredLogger};
use crate::types::lifecycle::LifecycleEntry;

/// Maximum number of retries before a bundle chain is abandoned.
pub const MAX_RETRIES: u32 = 3;

/// Tracks the full lifecycle of submitted bundles across all Solana commitment levels.
#[derive(Clone)]
pub struct LifecycleTracker {
    entries: Arc<DashMap<String, LifecycleEntry>>,
    sig_to_bundle: Arc<DashMap<String, String>>,
    log_file: String,
    logger: StructuredLogger,
    rpc_client: Arc<RpcClient>,
    /// Cache of recent block times to avoid per-update RPC calls.
    block_time_cache: Arc<DashMap<u64, i64>>,
}

impl LifecycleTracker {
    pub fn new(log_file: &str, logger: StructuredLogger, rpc_client: Arc<RpcClient>) -> Self {
        let entries = DashMap::new();
        let sig_to_bundle = DashMap::new();

        // Load existing lifecycle entries from disk if available.
        if let Ok(content) = std::fs::read_to_string(log_file) {
            if let Ok(loaded) = serde_json::from_str::<Vec<LifecycleEntry>>(&content) {
                for entry in loaded {
                    for sig in &entry.signatures {
                        sig_to_bundle.insert(sig.clone(), entry.bundle_id.clone());
                    }
                    entries.insert(entry.bundle_id.clone(), entry);
                }
            }
        }

        Self {
            entries: Arc::new(entries),
            sig_to_bundle: Arc::new(sig_to_bundle),
            log_file: log_file.to_string(),
            logger,
            rpc_client,
            block_time_cache: Arc::new(DashMap::new()),
        }
    }

    /// Record a new bundle submission in the tracker.
    pub async fn record_submission(
        &self,
        intent_id: String,
        bundle_id: String,
        slot: u64,
        tip: u64,
        signatures: Vec<String>,
        last_valid_block_height: u64,
        retry_count: u32,
        history_summary: String,
        block_height: Option<u64>,
    ) {
        let entry = LifecycleEntry {
            intent_id,
            bundle_id: bundle_id.clone(),
            slot_submitted: slot,
            block_height_submitted: block_height,
            submitted_at: Utc::now(),
            tip_lamports: tip,
            last_valid_block_height: Some(last_valid_block_height),
            signatures: signatures.clone(),
            retry_count,
            history_summary,
            ..Default::default()
        };
        self.entries.insert(bundle_id.clone(), entry);

        for sig in signatures {
            self.sig_to_bundle.insert(sig, bundle_id.clone());
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
        
        if lower.contains("blockhashnotfound") || lower.contains("blockhash not found") || lower.contains("blockhash expired") {
            "expired_blockhash".to_string()
        } else if lower.contains("insufficientfunds") || lower.contains("insufficient funds") || lower.contains("insufficient lamports") {
            "insufficient_funds".to_string()
        } else if lower.contains("computationalbudgetexceeded") || lower.contains("computational budget exceeded") || lower.contains("exceeded compute") {
            "compute_exceeded".to_string()
        } else if lower.contains("alreadyprocessed") || lower.contains("already processed") {
            "already_processed".to_string()
        } else if lower.contains("bundle rejected") || lower.contains("bundledrop") || lower.contains("dropped") {
            "bundle_rejected".to_string()
        } else if lower.contains("leader missed") || lower.contains("slot missed") {
            "leader_missed".to_string()
        } else if lower.contains("rpc error") || lower.contains("server error") || lower.contains("502") || lower.contains("503") || lower.contains("429") {
            "rpc_failure".to_string()
        } else if lower.contains("timeout") || lower.contains("deadline exceeded") {
            "network_timeout".to_string()
        } else if lower.contains("fee") && (lower.contains("too low") || lower.contains("insufficient")) {
            "insufficient_priority_fee".to_string()
        } else if lower.contains("simulation failed") || lower.contains("instructionerror") || lower.contains("program error") {
            "simulation_failure".to_string()
        } else if lower.contains("accountinuse") || lower.contains("account in use") || lower.contains("accountnotfound") || lower.contains("invalid account") {
            "account_error".to_string()
        } else {
            "unknown_failure".to_string()
        }
    }

    /// Get a cached or fetched block time for a given slot.
    async fn get_block_time_cached(&self, slot: u64) -> i64 {
        // Check cache first
        if let Some(cached) = self.block_time_cache.get(&slot) {
            return *cached;
        }

        // Fetch from RPC
        match self.rpc_client.get_block_time(slot).await {
            Ok(ts) => {
                // Cache the result (limit cache size to prevent unbounded growth)
                if self.block_time_cache.len() > 1000 {
                    // Remove oldest entries (rough eviction)
                    let keys: Vec<u64> = self.block_time_cache.iter().take(200).map(|e| *e.key()).collect();
                    for k in keys {
                        self.block_time_cache.remove(&k);
                    }
                }
                self.block_time_cache.insert(slot, ts);
                ts
            }
            Err(_) => Utc::now().timestamp(),
        }
    }

    /// Update the commitment status of a bundle.
    pub async fn update_status(&self, bundle_id: &str, commitment: &str, slot: u64) {
        // Fetch block time with caching
        let block_time_sec = self.get_block_time_cached(slot).await;

        let event = {
            let mut entry = match self.entries.get_mut(bundle_id) {
                Some(e) => e,
                None => return,
            };

            // Don't update failed bundles (unless marking as failed)
            if entry.status == "failed" && commitment != "failed" {
                return;
            }

            // Real timestamp from the network block time
            let real_timestamp =
                chrono::DateTime::from_timestamp(block_time_sec, 0).unwrap_or(Utc::now());
            // Ensure timestamp is not before submitted_at
            let ts = if real_timestamp < entry.submitted_at {
                entry.submitted_at
                    + chrono::Duration::milliseconds(
                        (slot.saturating_sub(entry.slot_submitted)) as i64 * 400,
                    )
            } else {
                real_timestamp
            };

            match commitment {
                "processed" if entry.processed_at.is_none() => {
                    entry.processed_at = Some(ts);
                    entry.processed_slot = Some(slot);
                    let lat = (ts - entry.submitted_at).num_milliseconds();
                    entry.latency_processed_ms = Some(lat);
                    let previous = entry.status.clone();
                    if Self::commitment_ord("processed") > Self::commitment_ord(&entry.status) {
                        entry.status = "processed".to_string();
                    }
                    Some(OperationalEvent::TransactionEvent {
                        transaction_id: entry.intent_id.clone(),
                        slot,
                        block_height: entry.block_height_submitted,
                        timestamp: ts,
                        lifecycle_state: "processed".to_string(),
                        latency_delta_ms: Some(lat),
                        previous_state: previous,
                        next_state: "processed".to_string(),
                        state_transition_valid: true,
                        transition_reason: "Observed processed commitment in Yellowstone stream".to_string(),
                        details: Some(format!("Bundle ID: {}", bundle_id)),
                    })
                }
                "confirmed" if entry.confirmed_at.is_none() => {
                    entry.confirmed_at = Some(ts);
                    entry.confirmed_slot = Some(slot);
                    let lat_from_submitted = (ts - entry.submitted_at).num_milliseconds();
                    entry.latency_confirmed_ms = Some(lat_from_submitted);

                    let delta = entry.processed_at.map(|p| (ts - p).num_milliseconds());
                    if let Some(d) = delta {
                        entry.latency_processed_to_confirmed_ms = Some(d);
                    }

                    let previous = entry.status.clone();
                    if Self::commitment_ord("confirmed") > Self::commitment_ord(&entry.status) {
                        entry.status = "confirmed".to_string();
                    }
                    Some(OperationalEvent::TransactionEvent {
                        transaction_id: entry.intent_id.clone(),
                        slot,
                        block_height: entry.block_height_submitted,
                        timestamp: ts,
                        lifecycle_state: "confirmed".to_string(),
                        latency_delta_ms: delta,
                        previous_state: previous,
                        next_state: "confirmed".to_string(),
                        state_transition_valid: true,
                        transition_reason: "Observed confirmed commitment in Yellowstone stream".to_string(),
                        details: Some(format!("Bundle ID: {}", bundle_id)),
                    })
                }
                "finalized" if entry.finalized_at.is_none() => {
                    entry.finalized_at = Some(ts);
                    entry.finalized_slot = Some(slot);
                    let lat = (ts - entry.submitted_at).num_milliseconds();
                    entry.latency_finalized_ms = Some(lat);
                    
                    let delta = entry.confirmed_at.map(|c| (ts - c).num_milliseconds());

                    let previous = entry.status.clone();
                    if Self::commitment_ord("finalized") > Self::commitment_ord(&entry.status) {
                        entry.status = "finalized".to_string();
                    }
                    Some(OperationalEvent::TransactionEvent {
                        transaction_id: entry.intent_id.clone(),
                        slot,
                        block_height: entry.block_height_submitted,
                        timestamp: ts,
                        lifecycle_state: "finalized".to_string(),
                        latency_delta_ms: delta,
                        previous_state: previous,
                        next_state: "finalized".to_string(),
                        state_transition_valid: true,
                        transition_reason: "Observed finalized commitment in Yellowstone stream".to_string(),
                        details: Some(format!("Bundle ID: {}", bundle_id)),
                    })
                }
                "failed" if entry.status != "failed" => {
                    let previous = entry.status.clone();
                    entry.status = "failed".to_string();
                    Some(OperationalEvent::TransactionEvent {
                        transaction_id: entry.intent_id.clone(),
                        slot,
                        block_height: entry.block_height_submitted,
                        timestamp: Utc::now(),
                        lifecycle_state: "failed".to_string(),
                        latency_delta_ms: None,
                        previous_state: previous,
                        next_state: "failed".to_string(),
                        state_transition_valid: true,
                        transition_reason: "Transaction error observed via stream".to_string(),
                        details: Some("Transaction error observed via stream".to_string()),
                    })
                }
                _ => None,
            }
        }; // Write lock dropped here

        if let Some(ref evt) = event {
            self.logger.log(evt).await;
            match evt {
                OperationalEvent::CommitmentUpdate {
                    bundle_id,
                    commitment,
                    slot,
                    latency_ms,
                    ..
                } => {
                    info!(
                        "{} Commitment: {} → {} at slot {} (latency: {:?}ms)",
                        "[TRACKER]".green(), bundle_id, commitment, slot, latency_ms
                    );
                }
                OperationalEvent::FailureDetected {
                    bundle_id,
                    failure_type,
                    ..
                } => {
                    warn!("{} Failure: {} — {}", "[TRACKER]".red(), bundle_id, failure_type);
                }
                _ => {}
            }
        }
    }

    /// Check if there are any pending bundles.
    pub fn has_pending_bundles(&self) -> bool {
        self.entries.iter().any(|e| e.status == "pending" || e.status == "processed" || e.status == "confirmed")
    }

    /// Advance commitment status of transactions based on slot finalization.
    pub async fn advance_commitments_by_slot(&self, slot: u64, status: i32) {
        let commitment = match status {
            0 => "processed",
            1 => "confirmed",
            2 => "finalized",
            _ => return,
        };
        
        let mut updates = Vec::new();
        for entry in self.entries.iter() {
            if let Some(landed_slot) = entry.processed_slot {
                if landed_slot <= slot {
                    if Self::commitment_ord(commitment) > Self::commitment_ord(&entry.status) {
                        updates.push((entry.bundle_id.clone(), commitment.to_string(), landed_slot));
                    }
                }
            }
        }
        
        for (bundle_id, comm, landed_slot) in updates {
            self.update_status(&bundle_id, &comm, landed_slot).await;
        }
    }

    /// Record a failure with classification for a specific bundle.
    pub async fn update_status_failed(&self, bundle_id: &str, error_msg: &str, slot: u64) {
        let failure_type = Self::classify_failure(error_msg);
        let event = {
            if let Some(mut entry) = self.entries.get_mut(bundle_id) {
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
            warn!("{} Classified failure for {}: {}", "[TRACKER]".red(), bundle_id, error_msg);
        }
    }

    /// Resolve a transaction signature to its bundle ID and update status.
    pub async fn update_status_by_sig(&self, signature: &str, commitment: &str, slot: u64) {
        let bundle_id = self.sig_to_bundle.get(signature).map(|r| r.clone());
        if let Some(bid) = bundle_id {
            self.update_status(&bid, commitment, slot).await;
        }
    }

    /// Resolve a transaction signature to its bundle ID and update with classified failure.
    pub async fn update_failure_by_sig(&self, signature: &str, error_msg: &str, slot: u64) {
        let bundle_id = self.sig_to_bundle.get(signature).map(|r| r.clone());
        if let Some(bid) = bundle_id {
            self.update_status_failed(&bid, error_msg, slot).await;
        }
    }

    /// Retrieve a copy of a lifecycle entry using a transaction signature.
    pub fn get_entry_by_sig(&self, signature: &str) -> Option<LifecycleEntry> {
        let bundle_id = self.sig_to_bundle.get(signature).map(|r| r.clone())?;
        self.entries.get(&bundle_id).map(|e| e.value().clone())
    }

    /// Record a failure for a specific bundle.
    pub async fn record_failure(&self, bundle_id: &str, failure_type: String) {
        if let Some(mut entry) = self.entries.get_mut(bundle_id) {
            entry.status = "failed".to_string();
            entry.failure_type = Some(failure_type.clone());
            warn!("{} Recorded failure for {}: {}", "[TRACKER]".red(), bundle_id, failure_type);
        }
    }

    /// Check for expired blockhashes using **block height** (NOT slot number).
    pub async fn check_expiries(
        &self,
        current_block_height: u64,
    ) -> Vec<(String, String, u64, u64, i64, u32, String)> {
        let mut expired = Vec::new();
        for mut entry in self.entries.iter_mut() {
            let bid = entry.key().clone();
            if entry.status == "pending" || entry.status == "processed" {
                if let Some(lvbh) = entry.last_valid_block_height {
                    if current_block_height > lvbh {
                        entry.status = "failed".to_string();
                        entry.failure_type = Some("expired_blockhash".to_string());
                        let age_ms = (Utc::now() - entry.submitted_at).num_milliseconds();
                        warn!(
                            "{} Blockhash expiry detected for bundle {} (block_height {} > last_valid {})",
                            "[TRACKER]".red(), bid, current_block_height, lvbh
                        );
                        expired.push((
                            entry.intent_id.clone(),
                            bid,
                            entry.slot_submitted,
                            entry.tip_lamports,
                            age_ms,
                            entry.retry_count,
                            entry.history_summary.clone(),
                        ));
                    }
                }
            }
        }
        expired
    }

    /// Poll `getSignatureStatuses` for pending/processed bundles as secondary confirmation.
    pub async fn poll_signature_statuses(&self) {
        // Collect signatures that still need confirmation
        let sigs_to_check: Vec<(String, String)> = {
            self.entries
                .iter()
                .filter(|e| {
                    e.status == "pending"
                        || e.status == "processed"
                        || e.status == "confirmed"
                })
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
                            self.update_status(bid, &commitment_str, status.slot)
                                .await;
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
        let values: Vec<_> = self.entries.iter().map(|e| e.value().clone()).collect();
        let json = serde_json::to_string_pretty(&values)?;
        tokio::fs::write(&self.log_file, json).await?;
        info!("{} Lifecycle logs saved ({} entries)", "[TRACKER]".green(), values.len());
        Ok(())
    }

    /// Get summary statistics for logging/display.
    pub async fn get_stats(&self) -> (usize, usize, usize, usize, usize) {
        let mut pending = 0;
        let mut processed = 0;
        let mut confirmed = 0;
        let mut finalized = 0;
        let mut failed = 0;
        for entry in self.entries.iter() {
            match entry.status.as_str() {
                "pending" => pending += 1,
                "processed" => processed += 1,
                "confirmed" => confirmed += 1,
                "finalized" => finalized += 1,
                "failed" => failed += 1,
                _ => {}
            }
        }
        (pending, processed, confirmed, finalized, failed)
    }
}
