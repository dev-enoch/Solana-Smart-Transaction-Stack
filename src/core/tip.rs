use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
use std::str::FromStr;
use solana_client::nonblocking::rpc_client::RpcClient;
use reqwest::Client;

/// Hard cap on dynamic tips to prevent runaway costs during extreme congestion.
/// 50_000_000 lamports = 0.05 SOL.
const MAX_TIP_LAMPORTS: u64 = 50_000_000;

/// Manages dynamic tip calculation for Jito bundles using real network data.
#[derive(Clone)]
pub struct TipManager {
    rpc_client: Arc<RpcClient>,
    http_client: Client,
    jito_url: String,
    recent_priority_fees: Arc<RwLock<Vec<u64>>>,
    tip_account_balances: Arc<RwLock<Vec<u64>>>,
    tip_accounts_cache: Arc<RwLock<Vec<Pubkey>>>,
}

impl TipManager {
    pub fn new(rpc_client: Arc<RpcClient>, jito_url: &str) -> Self {
        Self {
            rpc_client,
            http_client: Client::new(),
            jito_url: jito_url.to_string(),
            recent_priority_fees: Arc::new(RwLock::new(vec![])),
            tip_account_balances: Arc::new(RwLock::new(vec![])),
            tip_accounts_cache: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Fetch tip accounts from the Jito Block Engine API dynamically.
    pub async fn get_tip_accounts(&self) -> Result<Vec<Pubkey>> {
        // Try dynamic fetch from Jito
        match self.fetch_jito_tip_accounts().await {
            Ok(accounts) if !accounts.is_empty() => {
                info!("Fetched {} tip accounts from Jito API", accounts.len());
                *self.tip_accounts_cache.write().await = accounts.clone();
                Ok(accounts)
            }
            Ok(_) | Err(_) => {
                // Check cache first
                let cached = self.tip_accounts_cache.read().await;
                if !cached.is_empty() {
                    return Ok(cached.clone());
                }
                drop(cached);

                // Fall back to well-known Jito tip accounts
                warn!("Jito tip API unavailable — using hardcoded fallback accounts");
                let fallback = self.hardcoded_tip_accounts()?;
                *self.tip_accounts_cache.write().await = fallback.clone();
                Ok(fallback)
            }
        }
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
        let res = self.http_client
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

    /// Well-known Jito tip distribution accounts (fallback).
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

    /// Calculate dynamic tip using network congestion data and tip account activity.
    pub async fn calculate_dynamic_tip(
        &self,
        base_lamports: u64,
        congestion_factor: f64,
    ) -> Result<u64> {
        let fees = self.recent_priority_fees.read().await;
        let balances = self.tip_account_balances.read().await;

        // Signal 1: Average recent priority fees (network congestion proxy)
        let avg_fee = if fees.is_empty() {
            base_lamports
        } else {
            fees.iter().sum::<u64>() / fees.len() as u64
        };

        // Signal 2: Tip account balance activity (Jito tip competition indicator)
        // Higher accumulated balances suggest active tip competition
        let tip_pressure = if balances.is_empty() {
            1.0
        } else {
            let avg_balance = balances.iter().sum::<u64>() / balances.len() as u64;
            // Scale: higher balance → higher competition → increase tip
            let sol = avg_balance as f64 / 1_000_000_000.0;
            (1.0 + sol * 0.1).min(2.0) // Cap at 2x multiplier
        };

        let avg_balance_for_log = if balances.is_empty() {
            None
        } else {
            Some(balances.iter().sum::<u64>() / balances.len() as u64)
        };

        let uncapped_tip = (avg_fee as f64 * congestion_factor * tip_pressure)
            .max(base_lamports as f64) as u64;
        let dynamic_tip = uncapped_tip.min(MAX_TIP_LAMPORTS);

        if uncapped_tip > MAX_TIP_LAMPORTS {
            warn!(
                "Tip capped: {} → {} lamports (MAX_TIP_LAMPORTS={})",
                uncapped_tip, dynamic_tip, MAX_TIP_LAMPORTS
            );
        }

        info!(
            "Dynamic tip: {} lamports (avg_fee={}, congestion={:.2}, tip_pressure={:.2}, fees_sampled={}, avg_balance={:?})",
            dynamic_tip, avg_fee, congestion_factor, tip_pressure, fees.len(), avg_balance_for_log
        );

        Ok(dynamic_tip)
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
                        let mut sorted: Vec<u64> = fees
                            .into_iter()
                            .map(|f| f.prioritization_fee)
                            .collect();
                        sorted.sort_by(|a, b| b.cmp(a));
                        let top: Vec<u64> = sorted.into_iter().take(20).filter(|&f| f > 0).collect();
                        let count = top.len();
                        *recent_fees.write().await = top;
                        tracing::debug!("Updated priority fees: {} non-zero fees sampled", count);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to fetch priority fees: {}", e);
                    }
                }

                // Source 2: Jito tip account balances (real tip account data)
                let accounts = tip_accounts_cache.read().await.clone();
                if !accounts.is_empty() {
                    let mut balances = Vec::new();
                    // Sample up to 8 tip accounts using a single RPC call
                    let subset: Vec<solana_sdk::pubkey::Pubkey> = accounts.iter().take(8).cloned().collect();
                    match rpc_client.get_multiple_accounts(&subset).await {
                        Ok(accs) => {
                            for acc_opt in accs {
                                if let Some(acc) = acc_opt {
                                    balances.push(acc.lamports);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Failed to query multiple tip accounts balance: {}", e);
                        }
                    }
                    if !balances.is_empty() {
                        tracing::debug!(
                            "Updated tip account balances: {:?} lamports",
                            balances
                        );
                        *tip_balances.write().await = balances;
                    }
                }
            }
        });
    }
}
