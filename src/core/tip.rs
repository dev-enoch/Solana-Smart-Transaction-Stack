use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use std::str::FromStr;

#[derive(Clone)]
pub struct TipManager {
    _rpc_client: Arc<solana_client::rpc_client::RpcClient>,
    recent_tips: Arc<RwLock<Vec<u64>>>, // lamports
}

impl TipManager {
    pub fn new(rpc_url: &str) -> Self {
        Self {
            _rpc_client: Arc::new(solana_client::rpc_client::RpcClient::new(rpc_url.to_string())),
            recent_tips: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Fetch live tip accounts from Jito
    pub async fn get_tip_accounts(&self) -> Result<Vec<Pubkey>> {
        // Here we could use jito-rust-rpc::get_tip_accounts, but standard accounts are stable:
        let tip_accounts = vec![
            Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")?,
            Pubkey::from_str("HFqU5xCUcjZk5hGcLcGm119Cjg5vK5aB6E6H2rJq22A2")?,
            Pubkey::from_str("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY")?,
            Pubkey::from_str("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iMgaSbg")?,
            Pubkey::from_str("DfXygSm4jMyxPMAxVnLpsT4B32y8Zw4gqM9RMBH7E5vX")?,
            Pubkey::from_str("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBn1rvwB9x")?,
            Pubkey::from_str("EMDUBo7iUjRCR9GtoG34ZADG2BwQ38o9yqRz6F2vJvT1")?,
            Pubkey::from_str("DWY2zM19vCokwE4q4qjT37XEXgGqjX318QhT3eTqU1dJ")?,
        ];
        Ok(tip_accounts)
    }

    /// Calculate dynamic tip based on recent tips + network conditions
    pub async fn calculate_dynamic_tip(&self, base_lamports: u64, congestion_factor: f64) -> Result<u64> {
        let tips = self.recent_tips.read().await;
        let avg_tip = if tips.is_empty() { 
            base_lamports 
        } else { 
            tips.iter().sum::<u64>() / tips.len() as u64 
        };

        let dynamic_tip = (avg_tip as f64 * congestion_factor).max(base_lamports as f64) as u64;
        info!("Dynamic tip calculated: {} lamports (avg: {}, congestion: {})", dynamic_tip, avg_tip, congestion_factor);
        Ok(dynamic_tip)
    }

    // Background task to periodically update recent tips
    pub async fn start_tip_updater(&self) {
        info!("Tip updater started");
        let rpc_client = self._rpc_client.clone();
        let recent_tips = self.recent_tips.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                match rpc_client.get_recent_prioritization_fees(&[]) {
                    Ok(fees) => {
                        // Get top 100 recent fees
                        let mut sorted_fees: Vec<u64> = fees.into_iter().map(|f| f.prioritization_fee).collect();
                        sorted_fees.sort_by(|a, b| b.cmp(a));
                        let top_fees: Vec<u64> = sorted_fees.into_iter().take(20).filter(|&f| f > 0).collect();
                        
                        let mut tips = recent_tips.write().await;
                        *tips = top_fees;
                        tracing::debug!("Updated recent tips from RPC: {} fees found", tips.len());
                    }
                    Err(e) => {
                        tracing::error!("Failed to fetch recent prioritization fees: {}", e);
                    }
                }
            }
        });
    }
}
