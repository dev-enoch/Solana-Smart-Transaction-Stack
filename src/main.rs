use anyhow::Result;
use dotenv::dotenv;
use solana_smart_tx_stack_rs::ai::agent::AiAgent;
use solana_smart_tx_stack_rs::types::ai::FailureContext;
use solana_smart_tx_stack_rs::core::bundle::BundleBuilder;
use solana_smart_tx_stack_rs::core::tip::TipManager;
use solana_smart_tx_stack_rs::core::tracker::LifecycleTracker;
use solana_smart_tx_stack_rs::streaming::yellowstone::YellowstoneStreamer;
use solana_smart_tx_stack_rs::types::streaming::StreamEvent;
use tokio::sync::mpsc;
use tracing::info;
use solana_sdk::signature::Signer;
use std::sync::Arc;
use tokio::sync::Mutex;

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Intent {
    id: String,
    memo: String,
    retries: u32,
    override_tip: Option<u64>,
    fault_inject_bad_blockhash: bool,
    target_slot: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt::init();
    info!("Starting solana-smart-tx-stack-rs...");

    let endpoint = std::env::var("YELLOWSTONE_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:50051".to_string());
    let x_token = std::env::var("YELLOWSTONE_X_TOKEN").ok();
    let jito_url = std::env::var("JITO_BLOCK_ENGINE_URL")
        .unwrap_or_else(|_| "https://amsterdam.mainnet.block-engine.jito.wtf".to_string());
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://api.devnet.solana.com".to_string());
    let jito_validators_str = std::env::var("JITO_VALIDATORS").unwrap_or_default();
    let jito_validators: std::collections::HashSet<String> = jito_validators_str
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let ai_api_url = std::env::var("AI_API_URL")
        .unwrap_or_else(|_| "https://api.x.ai/v1/chat/completions".to_string());
    let ai_api_key = std::env::var("XAI_API_KEY").unwrap_or_default();

    let payer = if let Ok(key_str) = std::env::var("PRIVATE_KEY") {
        let bytes: Vec<u8> = serde_json::from_str(&key_str).expect("Invalid PRIVATE_KEY");
        solana_sdk::signature::Keypair::from_bytes(&bytes).expect("Invalid keypair bytes")
    } else {
        panic!("PRIVATE_KEY not found in .env");
    };

    info!("Using payer pubkey: {}", payer.pubkey());
    let rpc_client = Arc::new(solana_client::rpc_client::RpcClient::new(rpc_url.clone()));
    let tip_manager = TipManager::new(&rpc_url);
    
    let payer_bytes = payer.to_bytes();
    let bundle_payer = solana_sdk::signature::Keypair::from_bytes(&payer_bytes).unwrap();
    let bundle_builder = Arc::new(BundleBuilder::new(&jito_url, bundle_payer, tip_manager.clone(), &rpc_url));
    
    let tracker = Arc::new(LifecycleTracker::new("lifecycle_logs.json"));
    let ai_agent = Arc::new(AiAgent::new(ai_api_url, ai_api_key));

    tip_manager.start_tip_updater().await;

    let (event_tx, mut event_rx) = mpsc::channel::<StreamEvent>(100);
    info!("Connecting to Yellowstone at {}", endpoint);
    let streamer = YellowstoneStreamer::new(&endpoint, x_token, event_tx, &rpc_url, payer.pubkey().to_string(), jito_validators).await?;

    let mut streamer_task = streamer.clone();
    tokio::spawn(async move {
        if let Err(e) = streamer_task.start().await {
            tracing::error!("Streamer stopped: {:?}", e);
        }
    });

    let intent_queue = Arc::new(Mutex::new(vec![]));
    
    // Add initial test intents
    for i in 1..=3 {
        intent_queue.lock().await.push(Intent {
            id: format!("test_bundle_{}", i),
            memo: format!("smart-stack test bundle #{}", i),
            retries: 0,
            override_tip: None,
            fault_inject_bad_blockhash: i == 2, // inject fault on 2nd bundle
            target_slot: None,
        });
    }

    let tracker_clone = tracker.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let _ = tracker_clone.save_logs().await;
        }
    });

    while let Some(event) = event_rx.recv().await {
        match event {
            StreamEvent::Slot(update) => {
                let current_slot = update.slot;
                
                let expired_bundles = tracker.check_expiries(current_slot).await;
                for (bundle_id, _slot_submitted, tip) in expired_bundles {
                    let ctx = FailureContext {
                        bundle_id: bundle_id.clone(),
                        failure_type: "expired_blockhash".to_string(),
                        slot: current_slot,
                        tip,
                        latency: 5000,
                        extra: "Blockhash organically expired in network, transaction dropped".to_string(),
                    };
                    
                    let ai = ai_agent.clone();
                    let q = intent_queue.clone();
                    
                    tokio::spawn(async move {
                        if let Ok(decision) = ai.decide_on_failure(ctx).await {
                            match decision.action.as_str() {
                                "refresh_blockhash" => {
                                    let new_intent = Intent {
                                        id: format!("{}_retry", bundle_id),
                                        memo: "smart-stack retry".to_string(),
                                        retries: 1,
                                        override_tip: decision.new_tip_lamports,
                                        fault_inject_bad_blockhash: false,
                                        target_slot: None,
                                    };
                                    info!("AI autonomously resubmitting with refreshed blockhash and new tip {:?}", new_intent.override_tip);
                                    q.lock().await.push(new_intent);
                                },
                                "retry_higher_tip" => {
                                    let new_intent = Intent {
                                        id: format!("{}_retry", bundle_id),
                                        memo: "smart-stack retry".to_string(),
                                        retries: 1,
                                        override_tip: decision.new_tip_lamports,
                                        fault_inject_bad_blockhash: false, // old blockhash used but we assume retry
                                        target_slot: None,
                                    };
                                    info!("AI resubmitting with higher tip {:?}", new_intent.override_tip);
                                    q.lock().await.push(new_intent);
                                },
                                "wait" => {
                                    let new_intent = Intent {
                                        id: format!("{}_retry", bundle_id),
                                        memo: "smart-stack retry".to_string(),
                                        retries: 1,
                                        override_tip: decision.new_tip_lamports,
                                        fault_inject_bad_blockhash: false,
                                        target_slot: decision.wait_slots.map(|w| current_slot + w as u64),
                                    };
                                    info!("AI decided to wait. Will queue intent for target_slot {:?}", new_intent.target_slot);
                                    q.lock().await.push(new_intent);
                                },
                                "abort" | _ => {
                                    info!("AI decided to abort or returned unknown action. Dropping intent {}.", bundle_id);
                                }
                            }
                        }
                    });
                }
                
                let mut queue = intent_queue.lock().await;
                if !queue.is_empty() {
                    let mut should_submit = false;
                    let is_optimal = streamer.is_optimal_submission_window(current_slot).await;
                    
                    if let Some(target) = queue[0].target_slot {
                        if current_slot >= target && is_optimal {
                            should_submit = true;
                        }
                    } else if is_optimal {
                        should_submit = true;
                    }

                    if should_submit {
                        let intent = queue.remove(0);
                        info!("--- Submitting Intent: {} at optimal slot {} ---", intent.id, current_slot);
                        
                        let mut blockhash = rpc_client.get_latest_blockhash()?;
                        if intent.fault_inject_bad_blockhash {
                            info!("Injecting fault: using old blockhash!");
                            blockhash = solana_sdk::hash::Hash::default();
                        }
                        
                        let memo_str = format!("{} slot {}", intent.memo, current_slot);
                        let vtx = solana_smart_tx_stack_rs::core::memo::create_memo_tx(
                            &payer,
                            &memo_str,
                            &blockhash,
                            None
                        );
                    
                    let builder_clone = bundle_builder.clone();
                    let trk = tracker.clone();
                    
                    tokio::spawn(async move {
                        match builder_clone.build_and_submit(vec![vtx], current_slot).await {
                            Ok((bundle_id, signatures, mut last_valid_block_height)) => {
                                if intent.fault_inject_bad_blockhash {
                                    info!("Injecting fault: setting blockhash expiry to current_slot + 5 to organically simulate expiry");
                                    last_valid_block_height = current_slot + 5;
                                }

                                trk.record_submission(bundle_id.clone(), current_slot, intent.override_tip.unwrap_or(10_000), signatures, last_valid_block_height).await;
                            },
                            Err(e) => tracing::error!("Failed to submit bundle: {:?}", e)
                        }
                    });
                    }
                }
            },
            StreamEvent::Transaction(tx_update) => {
                if let Some(_err) = tx_update.error {
                    tracker.update_status_by_sig(&tx_update.signature, "failed", tx_update.slot).await;
                } else {
                    tracker.update_status_by_sig(&tx_update.signature, "processed", tx_update.slot).await;
                }
            }
        }
    }

    Ok(())
}
