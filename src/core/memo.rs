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
) -> VersionedTransaction {
    let mut instructions = vec![Instruction::new_with_bytes(
        Pubkey::from_str(MEMO_PROGRAM_ID).unwrap(),
        message.as_bytes(),
        vec![AccountMeta::new(payer.pubkey(), true)],
    )];

    if let Some(tip) = tip_ix {
        instructions.push(tip);
    }

    VersionedTransaction::try_new(
        solana_sdk::message::VersionedMessage::V0(
            solana_sdk::message::v0::Message::try_compile(
                &payer.pubkey(),
                &instructions,
                &[], // lookup tables
                *recent_blockhash,
            ).unwrap()
        ),
        &[payer]
    ).unwrap()
}
