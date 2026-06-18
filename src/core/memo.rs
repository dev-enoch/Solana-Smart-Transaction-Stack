use anyhow::{anyhow, Result};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::VersionedTransaction,
};
use std::str::FromStr;

// Memo Program ID (v2)
const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

pub fn create_memo_tx(
    payer: &Keypair,
    message: &str,
    recent_blockhash: &solana_sdk::hash::Hash,
    tip_ix: Option<Instruction>,   // for the last tx in bundle
    fault_injection: Option<String>,
) -> Result<VersionedTransaction> {
    let mut instructions = vec![Instruction::new_with_bytes(
        Pubkey::from_str(MEMO_PROGRAM_ID)
            .map_err(|e| anyhow!("Invalid Memo program ID: {}", e))?,
        message.as_bytes(),
        vec![AccountMeta::new(payer.pubkey(), true)],
    )];

    if let Some(fault) = fault_injection {
        if fault == "compute_exceeded" {
            instructions.push(solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(1));
        }
    }

    if let Some(tip) = tip_ix {
        instructions.push(tip);
    }

    let msg = solana_sdk::message::v0::Message::try_compile(
        &payer.pubkey(),
        &instructions,
        &[], // lookup tables
        *recent_blockhash,
    )
    .map_err(|e| anyhow!("Failed to compile memo message: {}", e))?;

    VersionedTransaction::try_new(
        solana_sdk::message::VersionedMessage::V0(msg),
        &[payer],
    )
    .map_err(|e| anyhow!("Failed to sign memo transaction: {}", e))
}
