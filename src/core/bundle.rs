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

use crate::core::tip::TipManager;

#[derive(Clone)]
pub struct BundleBuilder {
    jito_url: String,
    payer: Arc<Keypair>,
    tip_manager: TipManager,
    rpc_client: Arc<solana_client::rpc_client::RpcClient>,
    http_client: Client,
}

impl BundleBuilder {
    pub fn new(jito_url: &str, payer: Keypair, tip_manager: TipManager, rpc_url: &str) -> Self {
        Self {
            jito_url: jito_url.to_string(),
            payer: Arc::new(payer),
            tip_manager,
            rpc_client: Arc::new(solana_client::rpc_client::RpcClient::new(rpc_url.to_string())),
            http_client: Client::new(),
        }
    }

    pub async fn build_and_submit(
        &self,
        mut transactions: Vec<VersionedTransaction>,
        slot: u64,
    ) -> Result<(String, Vec<String>, u64)> {  
        let tip_account = self.tip_manager.get_tip_accounts().await?.first().cloned().unwrap();
        let tip_lamports = self.tip_manager.calculate_dynamic_tip(10_000, 1.5).await?; 

        info!("Building real tip transaction of {} lamports to {}", tip_lamports, tip_account);

        // Fetch real blockhash and last_valid_block_height
        let (blockhash, last_valid_block_height) = self.rpc_client.get_latest_blockhash_with_commitment(solana_sdk::commitment_config::CommitmentConfig::confirmed())
            .map_err(|e| anyhow!("Failed to get latest blockhash: {}", e))?;

        // Create tip instruction
        let tip_ix = system_instruction::transfer(
            &self.payer.pubkey(),
            &tip_account,
            tip_lamports,
        );

        let msg = Message::new(&[tip_ix], Some(&self.payer.pubkey()));
        let mut tx = Transaction::new_unsigned(msg);
        tx.sign(&[&*self.payer], blockhash);
        
        let versioned_tx = VersionedTransaction::from(tx);
        transactions.push(versioned_tx);

        info!("Constructing Jito bundle with {} transactions...", transactions.len());

        let mut encoded_txs = Vec::new();
        let mut signatures = Vec::new();
        for tx in transactions {
            if let Some(sig) = tx.signatures.first() {
                signatures.push(sig.to_string());
            }
            let serialized = bincode::serialize(&tx).map_err(|e| anyhow!("Bincode error: {}", e))?;
            encoded_txs.push(bs58::encode(serialized).into_string());
        }

        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [
                encoded_txs
            ]
        });

        let endpoint = format!("{}/api/v1/bundles", self.jito_url);
        let res = self.http_client.post(&endpoint)
            .json(&payload)
            .send()
            .await?;

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

        info!("Bundle submitted successfully! ID: {} at slot {}", bundle_id, slot);

        Ok((bundle_id, signatures, last_valid_block_height))
    }
}
