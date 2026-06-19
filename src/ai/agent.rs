use anyhow::Result;
use reqwest::Client;
use tracing::info;

use crate::types::ai::{AgentDecision, FailureContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderType {
    Gemini,
    OpenAi,
    Grok,
    Ollama,
}

impl ProviderType {
    pub fn detect(url: &str) -> Self {
        let lower = url.to_lowercase();
        if lower.contains("googleapis.com") || lower.contains("generativelanguage") {
            Self::Gemini
        } else if lower.contains("x.ai") {
            Self::Grok
        } else if lower.contains("localhost") || lower.contains("127.0.0.1") || lower.contains("11434") || lower.contains("ollama") {
            Self::Ollama
        } else {
            Self::OpenAi
        }
    }
}

#[derive(Clone)]
pub struct AiAgent {
    client: Client,
    api_url: String,
    api_key: String,
    model: String,
    fallback_url: Option<String>,
    fallback_key: Option<String>,
    fallback_model: Option<String>,
}

impl AiAgent {
    pub fn new(api_url: String, api_key: String, model: String) -> Self {
        let fallback_url = std::env::var("AI_FALLBACK_API_URL").ok();
        let fallback_key = std::env::var("AI_FALLBACK_API_KEY").ok();
        let fallback_model = std::env::var("AI_FALLBACK_MODEL").ok();
        Self {
            client: Client::new(),
            api_url,
            api_key,
            model,
            fallback_url,
            fallback_key,
            fallback_model,
        }
    }

    pub async fn decide_on_failure(&self, failure_context: FailureContext) -> Result<AgentDecision> {
        let prompt = self.build_failure_reasoning_prompt(&failure_context);

        let response = match self.call_llm_endpoint(&self.api_url, &self.api_key, &self.model, &prompt).await {
            Ok(resp) => resp,
            Err(e) => {
                if let (Some(fallback_url), Some(fallback_key)) = (&self.fallback_url, &self.fallback_key) {
                    let fallback_model = self.fallback_model.as_deref().unwrap_or("grok-3-mini-fast");
                    info!("Primary LLM failed: {:?}. Attempting fallback LLM...", e);
                    match self.call_llm_endpoint(fallback_url, fallback_key, fallback_model, &prompt).await {
                        Ok(resp) => resp,
                        Err(fallback_err) => {
                            tracing::error!("Fallback LLM also failed: {:?}", fallback_err);
                            return Err(anyhow::anyhow!("Primary error: {:?}, Fallback: {:?}", e, fallback_err));
                        }
                    }
                } else {
                    tracing::error!("LLM API call failed: {:?}", e);
                    return Err(anyhow::anyhow!("LLM failed: {:?}", e));
                }
            }
        };

        let decision: AgentDecision = serde_json::from_str(&response)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON response: {} - error: {}", response, e))?;
        info!("AI Decision: {} -> {}", decision.reasoning, decision.action);
        Ok(decision)
    }

    fn build_failure_reasoning_prompt(&self, ctx: &FailureContext) -> String {
        let history_block = if ctx.history_summary.trim().is_empty() {
            "No previous retry history for this intent."
        } else {
            &ctx.history_summary
        };

        format!(
            r#"You are an expert Solana transaction operator managing a Jito bundle submission pipeline.

Context for this failure:
- Bundle ID: {}
- Failure type: {}
- Current slot: {}
- Tip paid: {} lamports
- Latency to processed: {}ms
- Additional details: {}
- Retry History for this Intent: {}

Reason step by step about the root cause of this failure.
Consider the Solana transaction lifecycle, Jito bundle mechanics, and network conditions.
If you see a history of repeated failures of the same type, you MUST adapt your strategy (e.g., increase wait slots or tip significantly). Do not repeat the same failed strategy.
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
            ctx.bundle_id, ctx.failure_type, ctx.slot, ctx.tip, ctx.latency, ctx.extra, history_block
        )
    }

    async fn call_llm_endpoint(&self, url: &str, key: &str, model: &str, prompt: &str) -> Result<String> {
        let provider = ProviderType::detect(url);
        match provider {
            ProviderType::Gemini => {
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

                let request_url = if url.contains('?') {
                    format!("{}&key={}", url, key)
                } else {
                    format!("{}?key={}", url, key)
                };

                let res = self.client.post(&request_url)
                    .header("Content-Type", "application/json")
                    .json(&payload)
                    .send()
                    .await?;

                let status = res.status();
                if !status.is_success() {
                    let err = res.text().await?;
                    anyhow::bail!("Gemini API error ({}): {}", status, err);
                }

                let resp_json: serde_json::Value = res.json().await?;
                let content = resp_json["candidates"][0]["content"]["parts"][0]["text"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Failed to parse Gemini response"))?;

                Ok(self.clean_json_response(content))
            }
            ProviderType::OpenAi | ProviderType::Grok => {
                let payload = serde_json::json!({
                    "model": model,
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
                    "response_format": { "type": "json_object" }
                });

                let res = self.client.post(url)
                    .header("Content-Type", "application/json")
                    .header("Authorization", format!("Bearer {}", key))
                    .json(&payload)
                    .send()
                    .await?;

                let status = res.status();
                if !status.is_success() {
                    let err = res.text().await?;
                    anyhow::bail!("LLM API error ({}): {}", status, err);
                }

                let resp_json: serde_json::Value = res.json().await?;
                let content = resp_json["choices"][0]["message"]["content"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Failed to parse LLM response"))?;

                Ok(self.clean_json_response(content))
            }
            ProviderType::Ollama => {
                let payload = serde_json::json!({
                    "model": model,
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
                    "stream": false,
                    "format": "json"
                });

                let res = self.client.post(url)
                    .header("Content-Type", "application/json")
                    .json(&payload)
                    .send()
                    .await?;

                let status = res.status();
                if !status.is_success() {
                    let err = res.text().await?;
                    anyhow::bail!("Ollama error ({}): {}", status, err);
                }

                let resp_json: serde_json::Value = res.json().await?;
                let content = resp_json["message"]["content"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Failed to parse Ollama response"))?;

                Ok(self.clean_json_response(content))
            }
        }
    }

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

