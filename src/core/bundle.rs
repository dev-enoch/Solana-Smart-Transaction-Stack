use anyhow::{anyhow, Result};
use solana_sdk::{
    signature::{Keypair, Signer},
    transaction::{Transaction, VersionedTransaction},
    system_instruction,
    message::Message,
};
use std::sync::Arc;
use tracing::info;
use reqwest::Client;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::core::tip::TipManager;

#[derive(Clone)]
pub struct BundleBuilder {
    jito_url: String,
    payer: Arc<Keypair>,
    tip_manager: TipManager,
    rpc_client: Arc<RpcClient>,
    http_client: Client,
}

impl BundleBuilder {
    pub fn new(jito_url: &str, payer: Keypair, tip_manager: TipManager, rpc_client: Arc<RpcClient>) -> Self {
        Self {
            jito_url: jito_url.to_string(),
            payer: Arc::new(payer),
            tip_manager,
            rpc_client,
            http_client: Client::new(),
        }
    }

    /// Build and submit a Jito bundle with dynamic or AI-overridden tip.
    pub async fn build_and_submit(
        &self,
        mut transactions: Vec<VersionedTransaction>,
        slot: u64,
        override_tip: Option<u64>,
    ) -> Result<(String, Vec<String>, u64, u64)> {
        let tip_account = self
            .tip_manager
            .get_tip_accounts()
            .await?
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("No tip accounts available"))?;

        // Use AI-recommended tip if provided, otherwise calculate dynamically
        let tip_lamports = match override_tip {
            Some(tip) => {
                info!("Using AI-overridden tip: {} lamports", tip);
                tip
            }
            None => self.tip_manager.calculate_dynamic_tip(100_000, 1.5).await?,
        };

        info!(
            "Building tip transaction: {} lamports to {}",
            tip_lamports, tip_account
        );

        // Fetch fresh blockhash with confirmed commitment for maximum validity window.
        let (blockhash, last_valid_block_height) = self
            .rpc_client
            .get_latest_blockhash_with_commitment(
                solana_sdk::commitment_config::CommitmentConfig::confirmed(),
            )
            .await
            .map_err(|e| anyhow!("Failed to get latest blockhash: {}", e))?;

        // Build the tip transaction
        let tip_ix = system_instruction::transfer(&self.payer.pubkey(), &tip_account, tip_lamports);

        let msg = Message::new(&[tip_ix], Some(&self.payer.pubkey()));
        let mut tx = Transaction::new_unsigned(msg);
        tx.sign(&[&*self.payer], blockhash);

        let versioned_tx = VersionedTransaction::from(tx);
        transactions.push(versioned_tx);

        info!(
            "Constructing Jito bundle with {} transactions...",
            transactions.len()
        );

        // Serialize and encode all transactions
        let mut encoded_txs = Vec::new();
        let mut signatures = Vec::new();
        for tx in transactions {
            if let Some(sig) = tx.signatures.first() {
                signatures.push(sig.to_string());
            }
            let serialized =
                bincode::serialize(&tx).map_err(|e| anyhow!("Bincode serialization error: {}", e))?;
            encoded_txs.push(bs58::encode(serialized).into_string());
        }

        // Submit bundle to Jito Block Engine via sendBundle JSON-RPC
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [encoded_txs]
        });

        let endpoint = format!("{}/api/v1/bundles", self.jito_url);
        let res = self.http_client.post(&endpoint).json(&payload).send().await?;

        if !res.status().is_success() {
            let err_text = res.text().await?;
            anyhow::bail!("Jito Block Engine error: {}", err_text);
        }

        let resp_json: serde_json::Value = res.json().await?;

        if let Some(err) = resp_json.get("error") {
            anyhow::bail!("Bundle submission error: {}", err);
        }

        let bundle_id = resp_json["result"]
            .as_str()
            .unwrap_or("unknown_id")
            .to_string();

        info!(
            "Bundle submitted! ID: {} at slot {} (tip: {} lamports)",
            bundle_id, slot, tip_lamports
        );

        Ok((bundle_id, signatures, last_valid_block_height, tip_lamports))
    }
}
