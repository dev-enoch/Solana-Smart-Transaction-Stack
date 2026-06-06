use solana_sdk::signature::{Keypair, Signer};
use solana_client::rpc_client::RpcClient;

fn main() {
    let kp = Keypair::new();
    let bytes = kp.to_bytes();
    let pubkey = kp.pubkey();
    
    println!("PRIVATE_KEY=[{}]", bytes.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(","));
    println!("PUBKEY={}", pubkey);
    
    let rpc = RpcClient::new("https://api.devnet.solana.com".to_string());
    println!("Requesting airdrop...");
    match rpc.request_airdrop(&pubkey, 2_000_000_000) {
        Ok(sig) => println!("Airdrop success: {}", sig),
        Err(e) => println!("Airdrop failed: {:?}", e),
    }
}
