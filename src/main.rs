use anyhow::Result;
use dotenv::dotenv;
use solana_smart_tx_stack_rs::ai::agent::AiAgent;
use solana_smart_tx_stack_rs::core::bundle::BundleBuilder;
use solana_smart_tx_stack_rs::core::tip::TipManager;
use solana_smart_tx_stack_rs::core::tracker::{LifecycleTracker, MAX_RETRIES};
use solana_smart_tx_stack_rs::logging::{OperationalEvent, StructuredLogger};
use solana_smart_tx_stack_rs::streaming::yellowstone::YellowstoneStreamer;
use solana_smart_tx_stack_rs::types::ai::FailureContext;
use solana_smart_tx_stack_rs::types::streaming::StreamEvent;
use tokio::sync::mpsc;
use tracing::info;
use solana_sdk::signature::Signer;
use solana_client::nonblocking::rpc_client::RpcClient;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use chrono::Utc;

/// Maximum number of slots to wait for a Jito leader window before submitting anyway.
/// Prevents queue starvation when Jito validators are not in the upcoming schedule.
const MAX_WAIT_SLOTS: u64 = 100;

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
    /// The slot at which this intent was first queued, used for fallback submission timing.
    queued_at_slot: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt::init();
    info!("====== Starting solana-smart-tx-stack-rs ======");


    let network = std::env::var("NETWORK")
        .unwrap_or_else(|_| "devnet".to_string())
        .to_lowercase();
    let prefix = if network == "mainnet" { "MAINNET_" } else { "DEVNET_" };

    let endpoint = std::env::var(format!("{}YELLOWSTONE_ENDPOINT", prefix))
        .or_else(|_| std::env::var("YELLOWSTONE_ENDPOINT"))
        .unwrap_or_else(|_| "http://localhost:50051".to_string());
    
    let x_token = std::env::var(format!("{}YELLOWSTONE_X_TOKEN", prefix))
        .or_else(|_| std::env::var("YELLOWSTONE_X_TOKEN"))
        .ok();

    let jito_url = std::env::var(format!("{}JITO_BLOCK_ENGINE_URL", prefix))
        .or_else(|_| std::env::var("JITO_BLOCK_ENGINE_URL"))
        .unwrap_or_else(|_| "https://amsterdam.mainnet.block-engine.jito.wtf".to_string());

    let rpc_url = std::env::var(format!("{}RPC_URL", prefix))
        .or_else(|_| std::env::var("RPC_URL"))
        .unwrap_or_else(|_| {
            if network == "mainnet" {
                "https://api.mainnet-beta.solana.com".to_string()
            } else {
                "https://api.devnet.solana.com".to_string()
            }
        });
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


    // Single shared RPC client used by all components
    let rpc_client = Arc::new(RpcClient::new(rpc_url.clone()));

    let tip_manager = TipManager::new(rpc_client.clone(), &jito_url);

    let payer_bytes = payer.to_bytes();
    let bundle_payer = solana_sdk::signature::Keypair::from_bytes(&payer_bytes).unwrap();
    let bundle_builder = Arc::new(BundleBuilder::new(
        &jito_url,
        bundle_payer,
        tip_manager.clone(),
        rpc_client.clone(),
    ));

    let tracker = Arc::new(LifecycleTracker::new(
        "lifecycle_logs.json",
        logger.clone(),
        rpc_client.clone(),
    ));
    let ai_agent = Arc::new(AiAgent::new(ai_api_url, ai_api_key, ai_model));


    tip_manager.start_tip_updater().await;


    let (event_tx, mut event_rx) = mpsc::channel::<StreamEvent>(100);
    info!("Connecting to Yellowstone at {}", endpoint);
    let streamer = YellowstoneStreamer::new(
        &endpoint,
        x_token,
        event_tx,
        rpc_client.clone(),
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


    let intent_queue = Arc::new(Mutex::new(VecDeque::new()));

    {
        let mut queue = intent_queue.lock().await;

        // 8 normal bundle submissions
        for i in 1..=8 {
            queue.push_back(Intent {
                id: format!("bundle_{}", i),
                memo: format!("smart-stack bundle #{}", i),
                retries: 0,
                override_tip: None,
                fault_inject_early_expiry: false,
                target_slot: None,
                queued_at_slot: None,
            });
        }

        // Fault injection #1: simulated blockhash expiry
        queue.push_back(Intent {
            id: "bundle_9_fault_expiry".to_string(),
            memo: "smart-stack fault-inject expiry #1".to_string(),
            retries: 0,
            override_tip: None,
            fault_inject_early_expiry: true,
            target_slot: None,
            queued_at_slot: None,
        });

        // Fault injection #2: simulated blockhash expiry
        queue.push_back(Intent {
            id: "bundle_10_fault_expiry".to_string(),
            memo: "smart-stack fault-inject expiry #2".to_string(),
            retries: 0,
            override_tip: None,
            fault_inject_early_expiry: true,
            target_slot: None,
            queued_at_slot: None,
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

    // Periodic signature status polling for secondary confirmation via RPC
    let tracker_poll = tracker.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(8));
        loop {
            interval.tick().await;
            tracker_poll.poll_signature_statuses().await;
        }
    });


    info!("Entering main event loop...");

    while let Some(event) = event_rx.recv().await {
        match event {
            StreamEvent::Slot(update) => {
                let current_slot = update.slot;

                tracker.advance_commitments_by_slot(current_slot, update.status).await;


                let expired_bundles = tracker.check_expiries(current_slot).await;
                for (bundle_id, _slot_submitted, tip, age_ms, retry_count) in expired_bundles {
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

                    // Check retry cap before invoking AI
                    if retry_count >= MAX_RETRIES {
                        info!(
                            "Retry cap reached for {} (retries: {}/{}). Abandoning bundle chain.",
                            bundle_id, retry_count, MAX_RETRIES
                        );
                        logger
                            .log(&OperationalEvent::FailureDetected {
                                timestamp: Utc::now(),
                                bundle_id: bundle_id.clone(),
                                failure_type: "max_retries_exceeded".to_string(),
                                slot: current_slot,
                                details: format!(
                                    "Bundle chain abandoned after {} retries (cap: {})",
                                    retry_count, MAX_RETRIES
                                ),
                            })
                            .await;
                        continue;
                    }

                    let ctx = FailureContext {
                        bundle_id: bundle_id.clone(),
                        failure_type: "expired_blockhash".to_string(),
                        slot: current_slot,
                        tip,
                        latency: age_ms,
                        extra: "Blockhash expired before transaction was included in a block"
                            .to_string(),
                    };

                    let ai = ai_agent.clone();
                    let q = intent_queue.clone();
                    let log = logger.clone();
                    let next_retry = retry_count + 1;

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
                                            retries: next_retry,
                                            override_tip: decision.new_tip_lamports,
                                            fault_inject_early_expiry: false,
                                            target_slot: None,
                                            queued_at_slot: None,
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
                                            "AI → refresh_blockhash | tip: {:?} | retry {}/{}",
                                            new_intent.override_tip, next_retry, MAX_RETRIES
                                        );
                                        q.lock().await.push_back(new_intent);
                                    }
                                    "retry_higher_tip" => {
                                        let new_id = format!("{}_retry_tip", bundle_id);
                                        let new_intent = Intent {
                                            id: new_id.clone(),
                                            memo: "smart-stack retry (higher tip)".to_string(),
                                            retries: next_retry,
                                            override_tip: decision.new_tip_lamports,
                                            fault_inject_early_expiry: false,
                                            target_slot: None,
                                            queued_at_slot: None,
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
                                            "AI → retry_higher_tip | tip: {:?} | retry {}/{}",
                                            new_intent.override_tip, next_retry, MAX_RETRIES
                                        );
                                        q.lock().await.push_back(new_intent);
                                    }
                                    "wait" => {
                                        let target = decision.wait_slots.map(|w| current_slot + w);
                                        let new_id = format!("{}_retry_wait", bundle_id);
                                        let new_intent = Intent {
                                            id: new_id.clone(),
                                            memo: "smart-stack retry (waited for slot)".to_string(),
                                            retries: next_retry,
                                            override_tip: decision.new_tip_lamports,
                                            fault_inject_early_expiry: false,
                                            target_slot: target,
                                            queued_at_slot: None,
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

                                        info!("AI → wait until slot {:?} | retry {}/{}", target, next_retry, MAX_RETRIES);
                                        q.lock().await.push_back(new_intent);
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
                                    "AI decision failed for {}: {:?}. Activating fallback retry.",
                                    bundle_id,
                                    e
                                );

                                let fallback_tip = (tip as f64 * 1.5) as u64;
                                let new_id = format!("{}_retry_fallback", bundle_id);
                                let new_intent = Intent {
                                    id: new_id.clone(),
                                    memo: "smart-stack retry (AI fallback: refresh blockhash)"
                                        .to_string(),
                                    retries: next_retry,
                                    override_tip: Some(fallback_tip),
                                    fault_inject_early_expiry: false,
                                    target_slot: Some(current_slot + 5),
                                    queued_at_slot: None,
                                };

                                log.log(&OperationalEvent::RetryQueued {
                                    timestamp: Utc::now(),
                                    original_bundle_id: bundle_id.clone(),
                                    new_intent_id: new_id,
                                    reason: format!(
                                        "AI fallback: LLM unavailable ({:?}), auto-retry with refreshed blockhash and 1.5x tip",
                                        e
                                    ),
                                    new_tip: Some(fallback_tip),
                                })
                                .await;

                                info!(
                                    "AI fallback → refresh_blockhash + wait 5 slots | tip: {} | retry {}/{}",
                                    fallback_tip, next_retry, MAX_RETRIES
                                );
                                q.lock().await.push_back(new_intent);
                            }
                        }
                    });
                }


                let mut queue = intent_queue.lock().await;
                if !queue.is_empty() {
                    // Set queued_at_slot on first observation
                    if queue[0].queued_at_slot.is_none() {
                        queue[0].queued_at_slot = Some(current_slot);
                    }

                    let is_optimal =
                        streamer.is_optimal_submission_window(current_slot).await;
                    let mut should_submit = false;

                    if let Some(target) = queue[0].target_slot {
                        if current_slot >= target && is_optimal {
                            should_submit = true;
                        }
                        // Fallback: if we've waited past target + MAX_WAIT_SLOTS, submit anyway
                        if current_slot >= target + MAX_WAIT_SLOTS {
                            info!(
                                "Fallback submission: waited {}+ slots past target for {}",
                                MAX_WAIT_SLOTS, queue[0].id
                            );
                            should_submit = true;
                        }
                    } else if is_optimal {
                        should_submit = true;
                    } else if let Some(queued_at) = queue[0].queued_at_slot {
                        // Fallback: if intent has been waiting too long without a Jito window
                        if current_slot >= queued_at + MAX_WAIT_SLOTS {
                            info!(
                                "Fallback submission: no Jito window for {}+ slots for {}",
                                MAX_WAIT_SLOTS, queue[0].id
                            );
                            should_submit = true;
                        }
                    }

                    if should_submit {
                        let intent = queue.pop_front().unwrap();
                        drop(queue); // Release lock before async work

                        info!(
                            "--- Submitting Intent: {} at slot {} (retry {}/{}) ---",
                            intent.id, current_slot, intent.retries, MAX_RETRIES
                        );

                        // Fetch fresh blockhash using async RPC
                        let blockhash = match rpc_client.get_latest_blockhash().await {
                            Ok(bh) => bh,
                            Err(e) => {
                                tracing::error!("Failed to get blockhash: {:?}", e);
                                // Put intent back at front of queue
                                intent_queue.lock().await.push_front(intent);
                                continue;
                            }
                        };

                        let memo_str = format!("{} slot {}", intent.memo, current_slot);
                        let vtx = match solana_smart_tx_stack_rs::core::memo::create_memo_tx(
                            &payer,
                            &memo_str,
                            &blockhash,
                            None,
                        ) {
                            Ok(tx) => tx,
                            Err(e) => {
                                tracing::error!("Failed to create memo tx: {:?}", e);
                                intent_queue.lock().await.push_front(intent);
                                continue;
                            }
                        };

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
                                        intent_clone.retries,
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
                // Confirmed and finalized are tracked by the commitment poller
                // and the signature status polling task.
                if let Some(ref error_msg) = tx_update.error {
                    // Classify the failure type from the error message
                    tracker
                        .update_failure_by_sig(&tx_update.signature, error_msg, tx_update.slot)
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
