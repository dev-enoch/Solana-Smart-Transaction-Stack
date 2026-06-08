use solana_smart_tx_stack_rs::core::tracker::LifecycleTracker;
use solana_smart_tx_stack_rs::logging::StructuredLogger;
use solana_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_classify_failure() {
    assert_eq!(
        LifecycleTracker::classify_failure("Transaction simulation failed: Blockhash not found"),
        "expired_blockhash"
    );
    assert_eq!(
        LifecycleTracker::classify_failure("BlockhashNotFound"),
        "expired_blockhash"
    );
    assert_eq!(
        LifecycleTracker::classify_failure("Transfer: insufficient lamports 10000, need 150000"),
        "insufficient_funds"
    );
    assert_eq!(
        LifecycleTracker::classify_failure("Program Error: computational budget exceeded"),
        "compute_exceeded"
    );
    assert_eq!(
        LifecycleTracker::classify_failure("Some random unexpected error"),
        "transaction_error"
    );
}

#[tokio::test]
async fn test_record_submission_and_expiry() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_str().unwrap();
    
    // Create a dummy logger
    let logger = StructuredLogger::new("dummy_events.jsonl").unwrap();
    let rpc_client = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
    
    let tracker = LifecycleTracker::new(file_path, logger, rpc_client);
    
    // Record a bundle
    let bundle_id = "test_bundle_123".to_string();
    tracker.record_submission(
        bundle_id.clone(),
        1000, // slot_submitted
        150000,
        vec!["sig1".to_string()],
        1500, // last_valid_block_height (ignored by our slot heuristic)
        0,
    ).await;
    
    // Check expiry at slot 1150 (within 150 limit)
    let expiries = tracker.check_expiries(1150).await;
    assert!(expiries.is_empty(), "Bundle should not be expired at exactly +150 slots");
    
    // Check expiry at slot 1151 (outside 150 limit)
    let expiries = tracker.check_expiries(1151).await;
    assert_eq!(expiries.len(), 1, "Bundle should expire at > 150 slots");
    assert_eq!(expiries[0].0, bundle_id);
    
    // Clean up
    let _ = std::fs::remove_file("dummy_events.jsonl");
}
