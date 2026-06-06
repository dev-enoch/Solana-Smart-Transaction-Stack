use anyhow::Result;
use reqwest::Client;
use tracing::info;

use crate::types::ai::{AgentDecision, FailureContext};

/// AI Agent that makes autonomous operational decisions for failure recovery.
#[derive(Clone)]
pub struct AiAgent {
    client: Client,
    api_url: String,
    api_key: String,
    model: String,
}

impl AiAgent {
    pub fn new(api_url: String, api_key: String, model: String) -> Self {
        Self {
            client: Client::new(),
            api_url,
            api_key,
            model,
        }
    }

    /// Core method: Agent makes autonomous decision about a failure.
    pub async fn decide_on_failure(&self, failure_context: FailureContext) -> Result<AgentDecision> {
        let prompt = self.build_failure_reasoning_prompt(&failure_context);

        let response = match self.call_llm(&prompt).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!("LLM API call failed: {:?}. Aborting AI decision loop.", e);
                return Err(anyhow::anyhow!(
                    "LLM API failed, cannot reason about failure: {:?}",
                    e
                ));
            }
        };

        let decision: AgentDecision = serde_json::from_str(&response)
            .map_err(|e| anyhow::anyhow!("Failed to parse LLM JSON response: {} — raw: {}", e, response))?;
        info!(
            "AI Decision — Reasoning: {}\n  Action: {} | New tip: {:?} | Wait: {:?}",
            decision.reasoning, decision.action, decision.new_tip_lamports, decision.wait_slots
        );
        Ok(decision)
    }

    fn build_failure_reasoning_prompt(&self, ctx: &FailureContext) -> String {
        format!(
            r#"You are an expert Solana transaction operator managing a Jito bundle submission pipeline.

Context for this failure:
- Bundle ID: {}
- Failure type: {}
- Current slot: {}
- Tip paid: {} lamports
- Latency to processed: {}ms
- Additional details: {}

Reason step by step about the root cause of this failure.
Consider the Solana transaction lifecycle, Jito bundle mechanics, and network conditions.
Then decide the single best next action:

1. "refresh_blockhash" — The blockhash expired. Refresh it and resubmit with an appropriate tip.
2. "retry_higher_tip" — The tip was too low to compete. Increase the tip and resubmit.
3. "wait" — Network conditions are unfavorable. Wait for a better leader window. Specify how many slots to wait.
4. "abort" — The failure is unrecoverable or the cost of retrying exceeds the benefit.

Output valid JSON only (no markdown, no explanation outside the JSON):
{{
  "reasoning": "detailed chain of thought explaining your analysis",
  "root_cause": "concise root cause classification",
  "action": "refresh_blockhash | retry_higher_tip | wait | abort",
  "new_tip_lamports": optional number (suggested tip in lamports, or null),
  "wait_slots": optional number (slots to wait, or null)
}}"#,
            ctx.bundle_id, ctx.failure_type, ctx.slot, ctx.tip, ctx.latency, ctx.extra
        )
    }

    /// Detect API format based on the URL.
    fn is_google_api(&self) -> bool {
        self.api_url.contains("googleapis.com") || self.api_url.contains("generativelanguage")
    }

    /// Call the LLM with automatic API format detection.
    async fn call_llm(&self, prompt: &str) -> Result<String> {
        if self.is_google_api() {
            self.call_google_llm(prompt).await
        } else {
            self.call_openai_compatible_llm(prompt).await
        }
    }

    /// Google Generative AI format (Gemini, etc.)
    async fn call_google_llm(&self, prompt: &str) -> Result<String> {
        let payload = serde_json::json!({
            "contents": [{"parts": [{"text": prompt}]}],
            "systemInstruction": {
                "parts": [{"text": "You are a Solana transaction agent. Respond ONLY with valid JSON. Do not use markdown code blocks, just raw JSON."}]
            },
            "generationConfig": {
                "temperature": 0.0,
                "responseMimeType": "application/json"
            }
        });

        let url = if self.api_url.contains('?') {
            format!("{}&key={}", self.api_url, self.api_key)
        } else {
            format!("{}?key={}", self.api_url, self.api_key)
        };

        let res = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        let status = res.status();
        if !status.is_success() {
            let error_text = res.text().await?;
            anyhow::bail!("Google LLM API error ({}): {}", status, error_text);
        }

        let resp_json: serde_json::Value = res.json().await?;
        let content = resp_json["candidates"][0]["content"]["parts"][0]["text"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse Google LLM response content"))?;

        Ok(self.clean_json_response(content))
    }

    /// OpenAI-compatible format (works with xAI/Grok, OpenAI, Groq, Together, etc.)
    async fn call_openai_compatible_llm(&self, prompt: &str) -> Result<String> {
        let payload = serde_json::json!({
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a Solana transaction agent. Respond ONLY with valid JSON. Do not use markdown code blocks, just raw JSON."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"}
        });

        let res = self
            .client
            .post(&self.api_url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        let status = res.status();
        if !status.is_success() {
            let error_text = res.text().await?;
            anyhow::bail!("OpenAI-compatible LLM API error ({}): {}", status, error_text);
        }

        let resp_json: serde_json::Value = res.json().await?;
        let content = resp_json["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse OpenAI-compatible LLM response"))?;

        Ok(self.clean_json_response(content))
    }

    /// Strip markdown code block wrappers that LLMs sometimes add despite instructions.
    fn clean_json_response(&self, content: &str) -> String {
        content
            .trim()
            .strip_prefix("```json")
            .or_else(|| content.trim().strip_prefix("```"))
            .unwrap_or(content.trim())
            .strip_suffix("```")
            .unwrap_or(content.trim())
            .trim()
            .to_string()
    }
}
