use anyhow::Result;
use reqwest::Client;
use tracing::info;

use crate::types::ai::{AgentDecision, FailureContext};

#[derive(Clone)]
pub struct AiAgent {
    client: Client,
    api_url: String,
    api_key: String,
}

impl AiAgent {
    pub fn new(api_url: String, api_key: String) -> Self {
        Self { client: Client::new(), api_url, api_key }
    }

    /// Core method: Agent makes autonomous decision
    pub async fn decide_on_failure(&self, failure_context: FailureContext) -> Result<AgentDecision> {
        let prompt = self.build_failure_reasoning_prompt(&failure_context);
        
        let response = match self.call_llm(&prompt).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!("LLM API call failed: {:?}. Aborting AI decision loop.", e);
                return Err(anyhow::anyhow!("LLM API failed, cannot reason about failure: {:?}", e));
            }
        };

        let decision: AgentDecision = serde_json::from_str(&response)?;
        info!("AI Decision - Reasoning: {}\nAction: {}", decision.reasoning, decision.action);
        Ok(decision)
    }

    fn build_failure_reasoning_prompt(&self, ctx: &FailureContext) -> String {
        format!(r#"You are an expert Solana transaction operator.

Context:
- Bundle ID: {}
- Failure: {}
- Slot: {}
- Tip: {} lamports
- Latency to processed: {}ms
- Other details: {}

Reason step by step about the root cause.
Then decide the best next action:
1. Retry immediately with same tip
2. Retry with higher tip (suggest amount)
3. Refresh blockhash and retry
4. Wait for better leader slot
5. Abort

Output valid JSON only:
{{
  "reasoning": "detailed chain of thought",
  "root_cause": "...",
  "action": "retry_higher_tip | refresh_blockhash | wait | abort",
  "new_tip_lamports": optional number,
  "wait_slots": optional number
}}"#, 
            ctx.bundle_id, ctx.failure_type, ctx.slot, ctx.tip, ctx.latency, ctx.extra)
    }

    async fn call_llm(&self, prompt: &str) -> Result<String> {
        let payload = serde_json::json!({
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            "systemInstruction": {
                "parts": [{"text": "You are a Solana transaction agent. Respond ONLY with valid JSON. Do not use markdown code blocks, just raw JSON."}]
            },
            "generationConfig": {
                "temperature": 0.0,
                "responseMimeType": "application/json"
            }
        });

        // Use the query parameter if the url doesn't already have one, or just append the key
        let url = if self.api_url.contains("?") {
            format!("{}&key={}", self.api_url, self.api_key)
        } else {
            format!("{}?key={}", self.api_url, self.api_key)
        };

        let res = self.client.post(&url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        let status = res.status();
        if !status.is_success() {
            let error_text = res.text().await?;
            anyhow::bail!("LLM API error ({}): {}", status, error_text);
        }

        let resp_json: serde_json::Value = res.json().await?;
        let content = resp_json["candidates"][0]["content"]["parts"][0]["text"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse LLM response content"))?;

        // Clean any markdown wrapper (e.g., ```json ... ```)
        let clean_content = content
            .trim()
            .strip_prefix("```json")
            .unwrap_or(content)
            .strip_suffix("```")
            .unwrap_or(content)
            .trim();

        Ok(clean_content.to_string())
    }
}
