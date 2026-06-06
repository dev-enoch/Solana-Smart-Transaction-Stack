use anyhow::Result;
use dotenv::dotenv;
use solana_smart_tx_stack_rs::ai::agent::AiAgent;
use solana_smart_tx_stack_rs::core::bundle::BundleBuilder;
use solana_smart_tx_stack_rs::core::tip::TipManager;
use solana_smart_tx_stack_rs::core::tracker::LifecycleTracker;
use solana_smart_tx_stack_rs::logging::{OperationalEvent, StructuredLogger};
use solana_smart_tx_stack_rs::streaming::yellowstone::YellowstoneStreamer;
use solana_smart_tx_stack_rs::types::ai::FailureContext;
use solana_smart_tx_stack_rs::types::streaming::StreamEvent;
use tokio::sync::mpsc;
use tracing::info;
use solana_sdk::signature::Signer;
use solana_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;
use tokio::sync::Mutex;
use chrono::Utc;

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Intent {
    id: String,
    memo: String,
    retries: u32,
    override_tip: Option<u64>,
    /// If true, the tracker will set a very short last_valid_block_height
    /// to simulate blockhash expiry for testing the AI retry pipeline.
    fault_inject_early_expiry: bool,
    target_slot: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt::init();
    info!("====== Starting solana-smart-tx-stack-rs ======");


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
    let ai_api_key = std::env::var("AI_API_KEY")
        .or_else(|_| std::env::var("XAI_API_KEY"))
        .unwrap_or_default();
    let ai_model = std::env::var("AI_MODEL")
        .unwrap_or_else(|_| "grok-3".to_string());


    let payer = if let Ok(key_str) = std::env::var("PRIVATE_KEY") {
        let bytes: Vec<u8> = serde_json::from_str(&key_str).expect("Invalid PRIVATE_KEY format");
        solana_sdk::signature::Keypair::from_bytes(&bytes).expect("Invalid keypair bytes")
    } else {
        panic!("PRIVATE_KEY not found in .env — run `cargo run --bin keygen` to generate one");
    };
    info!("Payer pubkey: {}", payer.pubkey());


    let logger = StructuredLogger::new("operational_events.jsonl")
        .expect("Failed to create structured logger");


    let rpc_client = Arc::new(RpcClient::new(rpc_url.clone()));
    let tip_manager = TipManager::new(&rpc_url, &jito_url);

    let payer_bytes = payer.to_bytes();
    let bundle_payer = solana_sdk::signature::Keypair::from_bytes(&payer_bytes).unwrap();
    let bundle_builder = Arc::new(BundleBuilder::new(
        &jito_url,
        bundle_payer,
        tip_manager.clone(),
        &rpc_url,
    ));

    let tracker = Arc::new(LifecycleTracker::new(
        "lifecycle_logs.json",
        &rpc_url,
        logger.clone(),
    ));
    let ai_agent = Arc::new(AiAgent::new(ai_api_url, ai_api_key, ai_model));


    tip_manager.start_tip_updater().await;
    tracker.start_commitment_poller();


    let (event_tx, mut event_rx) = mpsc::channel::<StreamEvent>(100);
    info!("Connecting to Yellowstone at {}", endpoint);
    let streamer = YellowstoneStreamer::new(
        &endpoint,
        x_token,
        event_tx,
        &rpc_url,
        payer.pubkey().to_string(),
        jito_validators,
    )
    .await?;

    let mut streamer_task = streamer.clone();
    tokio::spawn(async move {
        if let Err(e) = streamer_task.start().await {
            tracing::error!("Yellowstone streamer stopped: {:?}", e);
        }
    });


    let intent_queue = Arc::new(Mutex::new(vec![]));

    {
        let mut queue = intent_queue.lock().await;

        // 8 normal bundle submissions
        for i in 1..=8 {
            queue.push(Intent {
                id: format!("bundle_{}", i),
                memo: format!("smart-stack bundle #{}", i),
                retries: 0,
                override_tip: None,
                fault_inject_early_expiry: false,
                target_slot: None,
            });
        }

        // Fault injection #1: simulated blockhash expiry
        queue.push(Intent {
            id: "bundle_9_fault_expiry".to_string(),
            memo: "smart-stack fault-inject expiry #1".to_string(),
            retries: 0,
            override_tip: None,
            fault_inject_early_expiry: true,
            target_slot: None,
        });

        // Fault injection #2: simulated blockhash expiry
        queue.push(Intent {
            id: "bundle_10_fault_expiry".to_string(),
            memo: "smart-stack fault-inject expiry #2".to_string(),
            retries: 0,
            override_tip: None,
            fault_inject_early_expiry: true,
            target_slot: None,
        });

        info!(
            "Intent queue initialized: {} bundles ({} with fault injection)",
            queue.len(),
            queue.iter().filter(|i| i.fault_inject_early_expiry).count()
        );
    }


    let tracker_persist = tracker.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let _ = tracker_persist.save_logs().await;
        }
    });


    info!("Entering main event loop...");

    while let Some(event) = event_rx.recv().await {
        match event {
            StreamEvent::Slot(update) => {
                let current_slot = update.slot;


                let expired_bundles = tracker.check_expiries(current_slot).await;
                for (bundle_id, _slot_submitted, tip) in expired_bundles {
                    // Log the failure event
                    logger
                        .log(&OperationalEvent::FailureDetected {
                            timestamp: Utc::now(),
                            bundle_id: bundle_id.clone(),
                            failure_type: "expired_blockhash".to_string(),
                            slot: current_slot,
                            details: "Blockhash expired — transaction dropped before landing"
                                .to_string(),
                        })
                        .await;

                    let ctx = FailureContext {
                        bundle_id: bundle_id.clone(),
                        failure_type: "expired_blockhash".to_string(),
                        slot: current_slot,
                        tip,
                        latency: 5000,
                        extra: "Blockhash expired before transaction was included in a block"
                            .to_string(),
                    };

                    let ai = ai_agent.clone();
                    let q = intent_queue.clone();
                    let log = logger.clone();

                    tokio::spawn(async move {
                        match ai.decide_on_failure(ctx).await {
                            Ok(decision) => {
                                // Log AI decision
                                log.log(&OperationalEvent::AiDecision {
                                    timestamp: Utc::now(),
                                    bundle_id: bundle_id.clone(),
                                    action: decision.action.clone(),
                                    reasoning: decision.reasoning.clone(),
                                    root_cause: decision.root_cause.clone(),
                                    new_tip: decision.new_tip_lamports,
                                    wait_slots: decision.wait_slots,
                                })
                                .await;

                                match decision.action.as_str() {
                                    "refresh_blockhash" => {
                                        let new_id = format!("{}_retry_bh", bundle_id);
                                        let new_intent = Intent {
                                            id: new_id.clone(),
                                            memo: "smart-stack retry (refreshed blockhash)"
                                                .to_string(),
                                            retries: 1,
                                            override_tip: decision.new_tip_lamports,
                                            fault_inject_early_expiry: false,
                                            target_slot: None,
                                        };

                                        log.log(&OperationalEvent::RetryQueued {
                                            timestamp: Utc::now(),
                                            original_bundle_id: bundle_id.clone(),
                                            new_intent_id: new_id,
                                            reason: "AI: refresh blockhash and retry".to_string(),
                                            new_tip: decision.new_tip_lamports,
                                        })
                                        .await;

                                        info!(
                                            "AI → refresh_blockhash | tip: {:?}",
                                            new_intent.override_tip
                                        );
                                        q.lock().await.push(new_intent);
                                    }
                                    "retry_higher_tip" => {
                                        let new_id = format!("{}_retry_tip", bundle_id);
                                        let new_intent = Intent {
                                            id: new_id.clone(),
                                            memo: "smart-stack retry (higher tip)".to_string(),
                                            retries: 1,
                                            override_tip: decision.new_tip_lamports,
                                            fault_inject_early_expiry: false,
                                            target_slot: None,
                                        };

                                        log.log(&OperationalEvent::RetryQueued {
                                            timestamp: Utc::now(),
                                            original_bundle_id: bundle_id.clone(),
                                            new_intent_id: new_id,
                                            reason: "AI: retry with higher tip".to_string(),
                                            new_tip: decision.new_tip_lamports,
                                        })
                                        .await;

                                        info!(
                                            "AI → retry_higher_tip | tip: {:?}",
                                            new_intent.override_tip
                                        );
                                        q.lock().await.push(new_intent);
                                    }
                                    "wait" => {
                                        let target = decision.wait_slots.map(|w| current_slot + w);
                                        let new_id = format!("{}_retry_wait", bundle_id);
                                        let new_intent = Intent {
                                            id: new_id.clone(),
                                            memo: "smart-stack retry (waited for slot)".to_string(),
                                            retries: 1,
                                            override_tip: decision.new_tip_lamports,
                                            fault_inject_early_expiry: false,
                                            target_slot: target,
                                        };

                                        log.log(&OperationalEvent::RetryQueued {
                                            timestamp: Utc::now(),
                                            original_bundle_id: bundle_id.clone(),
                                            new_intent_id: new_id,
                                            reason: format!(
                                                "AI: wait for target slot {:?}",
                                                target
                                            ),
                                            new_tip: decision.new_tip_lamports,
                                        })
                                        .await;

                                        info!("AI → wait until slot {:?}", target);
                                        q.lock().await.push(new_intent);
                                    }
                                    "abort" | _ => {
                                        info!(
                                            "AI → abort bundle {} (reason: {})",
                                            bundle_id, decision.reasoning
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "AI decision failed for {}: {:?}",
                                    bundle_id,
                                    e
                                );
                            }
                        }
                    });
                }


                let mut queue = intent_queue.lock().await;
                if !queue.is_empty() {
                    let is_optimal =
                        streamer.is_optimal_submission_window(current_slot).await;
                    let mut should_submit = false;

                    if let Some(target) = queue[0].target_slot {
                        if current_slot >= target && is_optimal {
                            should_submit = true;
                        }
                    } else if is_optimal {
                        should_submit = true;
                    }

                    if should_submit {
                        let intent = queue.remove(0);
                        drop(queue); // Release lock before async work

                        info!(
                            "--- Submitting Intent: {} at slot {} ---",
                            intent.id, current_slot
                        );

                        // Fetch fresh blockhash using async RPC
                        let blockhash = match rpc_client.get_latest_blockhash().await {
                            Ok(bh) => bh,
                            Err(e) => {
                                tracing::error!("Failed to get blockhash: {:?}", e);
                                // Put intent back at front of queue
                                intent_queue.lock().await.insert(0, intent);
                                continue;
                            }
                        };

                        let memo_str = format!("{} slot {}", intent.memo, current_slot);
                        let vtx = solana_smart_tx_stack_rs::core::memo::create_memo_tx(
                            &payer,
                            &memo_str,
                            &blockhash,
                            None,
                        );

                        let builder_clone = bundle_builder.clone();
                        let trk = tracker.clone();
                        let log = logger.clone();
                        let intent_clone = intent.clone();

                        tokio::spawn(async move {
                            match builder_clone
                                .build_and_submit(
                                    vec![vtx],
                                    current_slot,
                                    intent_clone.override_tip,
                                )
                                .await
                            {
                                Ok((
                                    bundle_id,
                                    signatures,
                                    mut last_valid_block_height,
                                    actual_tip,
                                )) => {
                                    // Fault injection: set artificially short expiry
                                    // to test the AI failure recovery pipeline
                                    if intent_clone.fault_inject_early_expiry {
                                        info!(
                                            "⚠ Fault injection: setting expiry to slot {} (current: {})",
                                            current_slot + 5,
                                            current_slot
                                        );
                                        last_valid_block_height = current_slot + 5;
                                    }

                                    trk.record_submission(
                                        bundle_id.clone(),
                                        current_slot,
                                        actual_tip,
                                        signatures.clone(),
                                        last_valid_block_height,
                                    )
                                    .await;

                                    log.log(&OperationalEvent::BundleSubmitted {
                                        timestamp: Utc::now(),
                                        bundle_id,
                                        slot: current_slot,
                                        tip_lamports: actual_tip,
                                        signatures,
                                        memo: intent_clone.memo,
                                    })
                                    .await;
                                }
                                Err(e) => {
                                    tracing::error!("Bundle submission failed: {:?}", e);
                                    log.log(&OperationalEvent::SubmissionError {
                                        timestamp: Utc::now(),
                                        intent_id: intent_clone.id,
                                        error: format!("{:?}", e),
                                        slot: current_slot,
                                    })
                                    .await;
                                }
                            }
                        });
                    }
                }
            }
            StreamEvent::Transaction(tx_update) => {
                // Stream delivers processed-level notifications.
                // Confirmed and finalized are tracked by the commitment poller.
                if tx_update.error.is_some() {
                    tracker
                        .update_status_by_sig(&tx_update.signature, "failed", tx_update.slot)
                        .await;
                } else {
                    tracker
                        .update_status_by_sig(&tx_update.signature, "processed", tx_update.slot)
                        .await;
                }
            }
        }
    }

    Ok(())
}
