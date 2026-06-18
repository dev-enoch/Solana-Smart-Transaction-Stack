use anyhow::{anyhow, Result};
use solana_sdk::{
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::VersionedTransaction,
};
use std::str::FromStr;

/// Memo Program v2 ID.
const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

/// Creates a memo transaction for inclusion in a Jito bundle.
pub fn create_memo_tx(
    payer: &Keypair,
    message: &str,
    recent_blockhash: &Hash,
    tip_ix: Option<Instruction>,
    fault_injection: Option<String>,
) -> Result<VersionedTransaction> {
    let mut instructions = vec![Instruction::new_with_bytes(
        Pubkey::from_str(MEMO_PROGRAM_ID)
            .map_err(|e| anyhow!("Invalid Memo program ID: {}", e))?,
        message.as_bytes(),
        vec![AccountMeta::new(payer.pubkey(), true)],
    )];

    let blockhash = match fault_injection.as_deref() {
        Some("expired_blockhash") => {
            tracing::warn!("FAULT INJECTION: Using deliberately expired blockhash (Hash::default)");
            Hash::default()
        }
        Some("compute_exceeded") => {
            tracing::warn!("FAULT INJECTION: Setting compute unit limit to 1");
            instructions.push(
                solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(1),
            );
            *recent_blockhash
        }
        _ => *recent_blockhash,
    };

    if let Some(tip) = tip_ix {
        instructions.push(tip);
    }

    let msg = solana_sdk::message::v0::Message::try_compile(
        &payer.pubkey(),
        &instructions,
        &[], // address lookup tables
        blockhash,
    )
    .map_err(|e| anyhow!("Failed to compile memo message: {}", e))?;

    VersionedTransaction::try_new(
        solana_sdk::message::VersionedMessage::V0(msg),
        &[payer],
    )
    .map_err(|e| anyhow!("Failed to sign memo transaction: {}", e))
}
