use solana_client::nonblocking::pubsub_client::PubsubClient;
use futures_util::StreamExt;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to wss://api.devnet.solana.com...");
    let pubsub = PubsubClient::new("wss://api.devnet.solana.com").await?;
    println!("Connected. Subscribing to slots...");
    let (mut slot_stream, _unsub) = pubsub.slot_subscribe().await?;
    
    println!("Waiting for slots...");
    for _ in 0..3 {
        match timeout(Duration::from_secs(5), slot_stream.next()).await {
            Ok(Some(slot)) => println!("Received slot: {}", slot.slot),
            Ok(None) => println!("Stream ended"),
            Err(_) => println!("Timeout waiting for slot"),
        }
    }
    Ok(())
}
