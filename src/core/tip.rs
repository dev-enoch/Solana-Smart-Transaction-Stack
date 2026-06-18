use anyhow::{anyhow, Result};
use rand::seq::SliceRandom;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Hard cap on dynamic tips to prevent runaway costs during extreme congestion.
const MAX_TIP_LAMPORTS: u64 = 50_000_000;

/// Minimum tip floor to ensure bundles are competitive even during quiet periods.
const MIN_TIP_LAMPORTS: u64 = 10_000;

/// Manages dynamic tip calculation for Jito bundles using real network data.
#[derive(Clone)]
pub struct TipManager {
    rpc_client: Arc<RpcClient>,
    http_client: reqwest::Client,
    jito_url: String,
    recent_priority_fees: Arc<RwLock<Vec<u64>>>,
    tip_account_balances: Arc<RwLock<Vec<u64>>>,
    tip_accounts_cache: Arc<RwLock<Vec<Pubkey>>>,
    /// Timestamp of last tip account API fetch for cache staleness.
    tip_accounts_fetched_at: Arc<RwLock<Option<std::time::Instant>>>,
}

impl TipManager {
    pub fn new(rpc_client: Arc<RpcClient>, jito_url: &str) -> Self {
        Self {
            rpc_client,
            http_client: reqwest::Client::new(),
            jito_url: jito_url.to_string(),
            recent_priority_fees: Arc::new(RwLock::new(vec![])),
            tip_account_balances: Arc::new(RwLock::new(vec![])),
            tip_accounts_cache: Arc::new(RwLock::new(vec![])),
            tip_accounts_fetched_at: Arc::new(RwLock::new(None)),
        }
    }

    /// Fetch tip accounts, using cache if fresh (< 5 minutes).
    pub async fn get_tip_accounts(&self) -> Result<Vec<Pubkey>> {
        // Check cache staleness
        let is_stale = {
            let fetched_at = self.tip_accounts_fetched_at.read().await;
            match *fetched_at {
                Some(ts) => ts.elapsed() > std::time::Duration::from_secs(300),
                None => true,
            }
        };

        if !is_stale {
            let cached = self.tip_accounts_cache.read().await;
            if !cached.is_empty() {
                return Ok(cached.clone());
            }
        }

        // Fetch from Jito API
        match self.fetch_jito_tip_accounts().await {
            Ok(accounts) if !accounts.is_empty() => {
                info!("Fetched {} tip accounts from Jito API", accounts.len());
                *self.tip_accounts_cache.write().await = accounts.clone();
                *self.tip_accounts_fetched_at.write().await = Some(std::time::Instant::now());
                Ok(accounts)
            }
            Ok(_) | Err(_) => {
                // Check cache (may be stale but still usable)
                let cached = self.tip_accounts_cache.read().await;
                if !cached.is_empty() {
                    warn!("Jito API unavailable — using cached tip accounts");
                    return Ok(cached.clone());
                }
                drop(cached);

                // Final fallback: well-known Jito tip accounts
                warn!("Jito tip API unavailable and cache empty — using hardcoded fallback accounts");
                let fallback = self.hardcoded_tip_accounts()?;
                *self.tip_accounts_cache.write().await = fallback.clone();
                Ok(fallback)
            }
        }
    }

    /// Select a random tip account from the available pool.
    pub async fn select_random_tip_account(&self) -> Result<Pubkey> {
        let accounts = self.get_tip_accounts().await?;
        accounts
            .choose(&mut rand::thread_rng())
            .cloned()
            .ok_or_else(|| anyhow!("No tip accounts available"))
    }

    /// Call the Jito Block Engine's `getTipAccounts` JSON-RPC method.
    async fn fetch_jito_tip_accounts(&self) -> Result<Vec<Pubkey>> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTipAccounts",
            "params": []
        });

        let endpoint = format!("{}/api/v1/bundles", self.jito_url);
        let res = self
            .http_client
            .post(&endpoint)
            .json(&payload)
            .send()
            .await
            .map_err(|e| anyhow!("Jito getTipAccounts request failed: {}", e))?;

        let resp: serde_json::Value = res
            .json()
            .await
            .map_err(|e| anyhow!("Failed to parse getTipAccounts response: {}", e))?;

        let accounts = resp["result"]
            .as_array()
            .ok_or_else(|| anyhow!("Invalid getTipAccounts response format"))?;

        let mut pubkeys = Vec::new();
        for account in accounts {
            if let Some(addr) = account.as_str() {
                pubkeys.push(Pubkey::from_str(addr)?);
            }
        }
        Ok(pubkeys)
    }

    /// Well-known Jito tip distribution accounts (fallback only).
    fn hardcoded_tip_accounts(&self) -> Result<Vec<Pubkey>> {
        Ok(vec![
            Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")?,
            Pubkey::from_str("HFqU5xCUcjZk5hGcLcGm119Cjg5vK5aB6E6H2rJq22A2")?,
            Pubkey::from_str("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY")?,
            Pubkey::from_str("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iMgaSbg")?,
            Pubkey::from_str("DfXygSm4jMyxPMAxVnLpsT4B32y8Zw4gqM9RMBH7E5vX")?,
            Pubkey::from_str("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBn1rvwB9x")?,
            Pubkey::from_str("EMDUBo7iUjRCR9GtoG34ZADG2BwQ38o9yqRz6F2vJvT1")?,
            Pubkey::from_str("DWY2zM19vCokwE4q4qjT37XEXgGqjX318QhT3eTqU1dJ")?,
        ])
    }

    /// Calculate a truly dynamic tip using network congestion data and tip account activity.
    pub async fn calculate_dynamic_tip(&self, base_lamports: u64) -> Result<u64> {
        let fees = self.recent_priority_fees.read().await;
        let balances = self.tip_account_balances.read().await;

        // ── Signal 1: p75 of recent priority fees ──────────────────────
        let (p75_fee, avg_fee, congestion_factor) = if fees.len() < 3 {
            (base_lamports, base_lamports, 1.0)
        } else {
            let mut sorted = fees.clone();
            sorted.sort();
            let p75_idx = (sorted.len() as f64 * 0.75) as usize;
            let p75 = sorted[p75_idx.min(sorted.len() - 1)];
            let avg = sorted.iter().sum::<u64>() / sorted.len() as u64;

            // Congestion factor from coefficient of variation
            let mean_f = avg as f64;
            if mean_f > 0.0 {
                let variance: f64 = sorted
                    .iter()
                    .map(|&f| (f as f64 - mean_f).powi(2))
                    .sum::<f64>()
                    / sorted.len() as f64;
                let cv = variance.sqrt() / mean_f;
                // cv < 0.5 → low congestion (factor ~1.0)
                let factor = (1.0 + cv * 0.5).min(2.5);
                (p75, avg, factor)
            } else {
                (p75, avg, 1.0)
            }
        };

        // ── Signal 2: Tip account balance activity ─────────────────────
        let tip_pressure = if balances.is_empty() {
            1.0
        } else {
            let avg_balance = balances.iter().sum::<u64>() / balances.len() as u64;
            let sol = avg_balance as f64 / 1_000_000_000.0;
            // Higher accumulated balance → more active competition → increase tip
            (1.0 + sol * 0.1).min(2.0)
        };

        let avg_balance_for_log = if balances.is_empty() {
            None
        } else {
            Some(balances.iter().sum::<u64>() / balances.len() as u64)
        };

        // Combine signals: use p75 as baseline, multiply by dynamic congestion and tip pressure
        let computed = (p75_fee as f64 * congestion_factor * tip_pressure)
            .max(base_lamports as f64)
            .max(MIN_TIP_LAMPORTS as f64) as u64;
        let dynamic_tip = computed.min(MAX_TIP_LAMPORTS);

        if computed > MAX_TIP_LAMPORTS {
            warn!(
                "Tip capped: {} → {} lamports (MAX_TIP_LAMPORTS={})",
                computed, dynamic_tip, MAX_TIP_LAMPORTS
            );
        }

        info!(
            "Dynamic tip: {} lamports (p75_fee={}, avg_fee={}, congestion={:.2}, tip_pressure={:.2}, fees_sampled={}, avg_balance={:?})",
            dynamic_tip, p75_fee, avg_fee, congestion_factor, tip_pressure, fees.len(), avg_balance_for_log
        );

        Ok(dynamic_tip)
    }

    /// Get a snapshot of current tip data for the AI agent.
    pub async fn get_tip_snapshot(
        &self,
    ) -> (Option<u64>, Option<u64>, Option<u64>, Option<u64>) {
        let fees = self.recent_priority_fees.read().await;
        let balances = self.tip_account_balances.read().await;

        let avg_fee = if fees.is_empty() {
            None
        } else {
            Some(fees.iter().sum::<u64>() / fees.len() as u64)
        };

        let p75_fee = if fees.len() < 3 {
            None
        } else {
            let mut sorted = fees.clone();
            sorted.sort();
            let idx = (sorted.len() as f64 * 0.75) as usize;
            Some(sorted[idx.min(sorted.len() - 1)])
        };

        let avg_balance = if balances.is_empty() {
            None
        } else {
            Some(balances.iter().sum::<u64>() / balances.len() as u64)
        };

        let current_tip = self.calculate_dynamic_tip(100_000).await.ok();

        (avg_fee, p75_fee, avg_balance, current_tip)
    }

    /// Start background task to periodically update tip data from real sources.
    pub async fn start_tip_updater(&self) {
        info!("Tip updater started (10s interval, dual-signal: priority fees + tip account balances)");
        let rpc_client = self.rpc_client.clone();
        let recent_fees = self.recent_priority_fees.clone();
        let tip_balances = self.tip_account_balances.clone();
        let tip_accounts_cache = self.tip_accounts_cache.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;

                // Source 1: Recent priority fees from Solana RPC
                match rpc_client.get_recent_prioritization_fees(&[]).await {
                    Ok(fees) => {
                        let non_zero: Vec<u64> = fees
                            .into_iter()
                            .map(|f| f.prioritization_fee)
                            .filter(|&f| f > 0)
                            .collect();
                        let count = non_zero.len();
                        *recent_fees.write().await = non_zero;
                        tracing::debug!("Updated priority fees: {} non-zero fees sampled", count);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to fetch priority fees: {}", e);
                    }
                }

                // Source 2: Jito tip account balances (real tip account data)
                let accounts = tip_accounts_cache.read().await.clone();
                if !accounts.is_empty() {
                    let subset: Vec<Pubkey> = accounts.iter().take(8).cloned().collect();
                    match rpc_client.get_multiple_accounts(&subset).await {
                        Ok(accs) => {
                            let balances: Vec<u64> = accs
                                .into_iter()
                                .filter_map(|a| a.map(|acc| acc.lamports))
                                .collect();
                            if !balances.is_empty() {
                                tracing::debug!(
                                    "Updated tip account balances: {} accounts sampled",
                                    balances.len()
                                );
                                *tip_balances.write().await = balances;
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Failed to query tip account balances: {}",
                                e
                            );
                        }
                    }
                }
            }
        });
    }
}
