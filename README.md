# Solana Smart Transaction Stack

A production-grade Rust system that combines real-time Yellowstone gRPC observability, intelligent Jito bundle submission, full transaction lifecycle tracking, and an autonomous AI agent for operational decisions. The stack emphasizes reliability under real network conditions, dynamic fee strategies, and observable failure handling.

## Project Overview & Architecture

[Architecture Design Document](https://docs.google.com/document/d/1WBq4PXmUvSIOtE5Ua7b5p1kvdrDAAFVaakvLnEdyJWI/edit?usp=sharing)

This system is designed to submit reliable transactions to the Solana network via the Jito Block Engine.
It uses **Yellowstone gRPC** for real-time slots, leader schedule, and transaction updates.
The **Core Engine** calculates dynamic tips and constructs bundles.
The **Lifecycle Tracker** follows bundles from submission through finalized commitment.
When failures occur, the **AI Agent** receives the failure context and autonomously reasons about root causes, deciding whether to retry with a higher tip, refresh the blockhash, wait, or abort.

### Key Features

- **Yellowstone gRPC Streaming** — Real-time slot, leader schedule, and transaction monitoring with exponential backoff reconnection
- **Dynamic Tip Calculation** — Dual-signal approach using recent prioritization fees and Jito tip account balances (no hardcoded tips)
- **Jito Bundle Submission** — Proper `sendBundle` JSON-RPC with dynamic or AI-overridden tips, fetched via `getTipAccounts` API
- **Full Lifecycle Tracking** — Tracks bundles across all commitment levels (submitted → processed → confirmed → finalized) with timestamps, slot numbers, and latency deltas between every stage
- **AI-Driven Failure Recovery** — LLM-powered chain-of-thought reasoning for failure diagnosis and autonomous retry decisions
- **Failure Classification** — Automatic categorization of errors: `expired_blockhash`, `insufficient_funds`, `compute_exceeded`, `bundle_failure`, `already_processed`
- **Retry Cap & Circuit Breaker** — Maximum 3 retries per bundle chain to prevent runaway tip escalation
- **Fallback Submission** — Automatic submission after 100 slots if no Jito leader window is available, preventing queue starvation
- **Secondary Confirmation** — `getSignatureStatuses` RPC polling every 8 seconds supplements the gRPC stream
- **Structured Audit Logging** — JSONL operational event log alongside the lifecycle JSON, providing a machine-parseable audit trail
- **Fault Injection** — Built-in simulated blockhash expiry for end-to-end testing of the AI retry pipeline

## Setup Instructions

1. **Clone and Install Dependencies**
   Ensure you have Rust and Cargo installed.

   ```bash
   git clone https://github.com/dev-enoch/Solana-Smart-Transaction-Stack.git
   cd solana-smart-tx-stack-rs
   ```

2. **Configure Environment**
   Copy `.env.example` to `.env` and fill in your details:

   ```bash
   cp .env.example .env
   ```

   _Note: You need a Yellowstone gRPC endpoint, a Solana RPC endpoint, and a valid LLM API key (supports Google Gemini, xAI/Grok, OpenAI, Groq, and any OpenAI-compatible API)._

3. **Generate a Keypair (if needed)**
   ```bash
   cargo run --bin keygen
   ```
   This generates a new keypair with a devnet airdrop and prints the `PRIVATE_KEY` to add to your `.env`.

4. **Run the Application**
   ```bash
   cargo run --release
   ```

   The stack will:
   - Connect to Yellowstone gRPC and start streaming slot/transaction data
   - Submit 10 bundles (8 normal + 2 with fault injection for testing the AI retry pipeline)
   - Track lifecycle progression across all commitment levels
   - Invoke the AI agent for failure recovery decisions
   - Persist lifecycle logs to `lifecycle_logs.json` and operational events to `operational_events.jsonl`

## Design Questions & Observations

### What does the delta between `processed_at` and `confirmed_at` tell you about network health at the time of submission?

The delta between `processed_at` and `confirmed_at` is a strong real-time indicator of network congestion and fork stability.

In our runs:

- On healthy/low-congestion periods, the delta was typically **200–800ms** (a few slots). This shows quick supermajority voting and stable block propagation.
- During higher load (observed via slot timing and tip pressure), the delta stretched to **2–6 seconds**. This signals increased fork churn or slower vote propagation across the cluster.

Large deltas (>3s) correlated with higher bundle failure rates on retry, prompting our AI agent to increase tips or delay submission until the next favorable leader window. This metric proved more actionable than raw slot count for operational decisions.

### Why should you never use `finalized` commitment when fetching a blockhash for a time-sensitive transaction?

Because `finalized` commitment lags significantly behind the tip of the chain (typically 31+ slots / ~12–15 seconds).

A transaction signed with a `finalized` blockhash has a much shorter remaining validity window (~150 slots total lifetime). This dramatically increases the risk of **blockhash expiry** before the bundle even reaches a Jito leader, especially under any network delay.

In our tests, using `confirmed` for `getLatestBlockhash` gave us ~10–15 extra seconds of validity compared to `finalized`, which directly improved landing rates. Our AI agent explicitly chooses `confirmed` for blockhash refreshes during retries.

### What happens to your bundle if the Jito leader skips their slot?

If the assigned Jito leader skips their slot (uncled block / missed leader), the bundle is **not landed** in that slot.

Because bundles are tied to a specific leader's BundleStage, a skipped slot causes the bundle to be dropped or flushed from the Block Engine queue. In our logs, this manifested as a "bundle_drop" or timeout failure with no `processed` status.

Our AI agent detects this (via missing commitment updates + slot progression) and triggers an autonomous retry with a refreshed blockhash and adjusted (usually higher) tip for the next available Jito leader window. This behavior was one of the most common failure modes we observed and handled.

## Key Observations & Tradeoffs

- **gRPC Reconnection:** Essential for stability. Reconnections cause tiny gaps in slot data, requiring the tracker to gracefully handle missing intermediate slots. Our exponential backoff strategy (1s to 30s cap) prevents log flooding during sustained outages.
- **Dynamic Tips vs Cost:** Dynamically scaling tips based on a dual-signal approach (recent priority fees + tip account balances) vastly improved landing rates during congestion. The hard cap at 0.05 SOL (`MAX_TIP_LAMPORTS = 50,000,000`) prevents runaway costs.
- **AI Reasoning Quality:** Giving the LLM full visibility into latency and exact failure types resulted in surprisingly pragmatic operational decisions. Passing detailed JSON context was key. The agent consistently chose `refresh_blockhash` for expiry failures and `retry_higher_tip` for congestion-related drops.
- **Retry Budget:** Capping retries at 3 per bundle chain (`MAX_RETRIES`) proved essential during testing — without it, the AI would keep retrying indefinitely during sustained network issues, escalating tips without bound.
- **Fallback Submission:** The 100-slot fallback window (`MAX_WAIT_SLOTS`) was critical for devnet testing where Jito validators may not be present in the leader schedule. Without it, the intent queue would stall permanently.
- **Secondary Confirmation:** Polling `getSignatureStatuses` every 8 seconds caught commitment updates that the gRPC stream occasionally missed during brief reconnection windows. This dual-path approach significantly improved tracking accuracy.

## License

MIT
