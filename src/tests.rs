#[cfg(test)]
mod tests {
    use crate::core::tracker::LifecycleTracker;
    use crate::streaming::yellowstone::YellowstoneStreamer;
    use solana_client::nonblocking::rpc_client::RpcClient;
    use std::sync::Arc;
    use std::collections::HashSet;

    #[test]
    fn test_classify_failure() {
        assert_eq!(
            LifecycleTracker::classify_failure("InstructionError: ComputationalBudgetExceeded"),
            "compute_exceeded"
        );
        assert_eq!(
            LifecycleTracker::classify_failure("BlockhashNotFound"),
            "expired_blockhash"
        );
        assert_eq!(
            LifecycleTracker::classify_failure("InsufficientFundsForRent"),
            "insufficient_funds"
        );
        assert_eq!(
            LifecycleTracker::classify_failure("Fee too low"),
            "insufficient_priority_fee"
        );
        assert_eq!(
            LifecycleTracker::classify_failure("Some unknown error occurred"),
            "unknown_failure"
        );
    }

    #[tokio::test]
    async fn test_optimal_submission_window() {
        let rpc_client = Arc::new(RpcClient::new("https://api.devnet.solana.com".to_string()));
        let mut jito_validators = HashSet::new();
        jito_validators.insert("validator_a".to_string());
        jito_validators.insert("validator_b".to_string());

        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let streamer = YellowstoneStreamer::new(
            "http://localhost:50051",
            None,
            tx,
            rpc_client,
            "payer_pubkey".to_string(),
            jito_validators,
        )
        .await
        .unwrap();

        // Populate leader schedule manually
        {
            let schedule = streamer.is_optimal_submission_window(100).await; // should be false
            assert!(!schedule);
        }
    }
}
