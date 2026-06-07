use solana_smart_tx_stack_rs::types::ai::AgentDecision;

#[test]
fn test_parse_ai_response_valid() {
    let raw_json = r#"{
        "reasoning": "The blockhash is simply too old.",
        "root_cause": "Blockhash expiration",
        "action": "refresh_blockhash",
        "new_tip_lamports": 500000,
        "wait_slots": null
    }"#;

    let decision: AgentDecision = serde_json::from_str(raw_json).expect("Failed to parse valid JSON");
    
    assert_eq!(decision.action, "refresh_blockhash");
    assert_eq!(decision.new_tip_lamports, Some(500000));
    assert_eq!(decision.wait_slots, None);
    assert_eq!(decision.root_cause, "Blockhash expiration");
}

#[test]
fn test_parse_ai_response_wait() {
    let raw_json = r#"{
        "reasoning": "Network is heavily congested.",
        "root_cause": "Congestion",
        "action": "wait",
        "new_tip_lamports": null,
        "wait_slots": 50
    }"#;

    let decision: AgentDecision = serde_json::from_str(raw_json).expect("Failed to parse valid JSON");
    
    assert_eq!(decision.action, "wait");
    assert_eq!(decision.new_tip_lamports, None);
    assert_eq!(decision.wait_slots, Some(50));
}

#[test]
fn test_parse_ai_response_invalid() {
    let invalid_json = r#"{
        "reasoning": "Missing fields"
    }"#;

    let result = serde_json::from_str::<AgentDecision>(invalid_json);
    assert!(result.is_err(), "Should fail to parse incomplete JSON");
}
