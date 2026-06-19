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

    #[test]
    fn test_provider_type_detection() {
        use crate::ai::agent::ProviderType;
        assert_eq!(ProviderType::detect("https://generativelanguage.googleapis.com/v1beta"), ProviderType::Gemini);
        assert_eq!(ProviderType::detect("https://api.x.ai/v1/chat/completions"), ProviderType::Grok);
        assert_eq!(ProviderType::detect("http://localhost:11434/api/chat"), ProviderType::Ollama);
        assert_eq!(ProviderType::detect("https://api.openai.com/v1/chat/completions"), ProviderType::OpenAi);
    }

    #[tokio::test]
    async fn test_advance_commitments_by_slot() {
        let tracker = LifecycleTracker::new(
            "test_lifecycle_logs.json",
            crate::logging::StructuredLogger::new("test_operational_events.jsonl").unwrap(),
            Arc::new(RpcClient::new("https://api.devnet.solana.com".to_string())),
        );

        tracker.record_submission(
            "intent_1".to_string(),
            "bundle_1".to_string(),
            100,
            150000,
            vec!["sig1".to_string()],
            200,
            0,
            "".to_string(),
            None,
        ).await;

        tracker.update_status("bundle_1", "processed", 102).await;
        let entry = tracker.get_entry_by_sig("sig1").unwrap();
        assert_eq!(entry.status, "processed");
        assert_eq!(entry.processed_slot, Some(102));

        tracker.advance_commitments_by_slot(105, 1).await;
        let entry = tracker.get_entry_by_sig("sig1").unwrap();
        assert_eq!(entry.status, "confirmed");

        let _ = std::fs::remove_file("test_lifecycle_logs.json");
        let _ = std::fs::remove_file("test_operational_events.jsonl");
    }
}
