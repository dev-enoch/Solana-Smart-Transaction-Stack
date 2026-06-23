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
- **Dual-Submission Fallback** — Simultaneously submits transactions via standard RPC (`send_transaction` with `skip_preflight: true`) to guarantee inclusion even if the Jito bundle is dropped or Jito leaders are scarce.
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

   **Where to get environment values:**
   - `NETWORK`: Set to `mainnet` or `devnet` to quickly switch environments.
   - `DEVNET_RPC_URL` / `MAINNET_RPC_URL`: Get a Solana RPC endpoint from providers like [Helius](https://helius.dev) or [QuickNode](https://quicknode.com).
   - `DEVNET_YELLOWSTONE_ENDPOINT` / `MAINNET_YELLOWSTONE_ENDPOINT`: Geyser/Yellowstone gRPC provider URL.
   - `DEVNET_YELLOWSTONE_X_TOKEN` / `MAINNET_YELLOWSTONE_X_TOKEN`: Token for gRPC access.
   - `DEVNET_JITO_BLOCK_ENGINE_URL` / `MAINNET_JITO_BLOCK_ENGINE_URL`: Standard Jito endpoints (e.g. `amsterdam.mainnet.block-engine.jito.wtf`).
   - `JITO_VALIDATORS`: Comma-separated list of known Jito validator pubkeys for leader targeting.
   - `PRIVATE_KEY`: Generate a testing keypair via `cargo run --bin keygen`.
   - `AI_API_URL`: The LLM provider API endpoint (e.g., `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent`).
   - `AI_API_KEY`: Your primary AI API key.
   - `AI_MODEL`: Set to the specific model string (e.g., `gemini-2.5-flash`).
   - `AI_FALLBACK_API_URL`, `AI_FALLBACK_API_KEY`, `AI_FALLBACK_MODEL`: Optional fallback provider details.

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

5. **Run the Dashboard**
   In a separate terminal, navigate to the dashboard directory, install dependencies, and start the Next.js real-time monitoring dashboard:

   ```bash
   cd dashboard
   yarn install
   yarn dev
   ```
   
   The dashboard will be available at `http://localhost:3000`.

## Design Questions & Observations

### What does the delta between `processed_at` and `confirmed_at` tell you about network health at the time of submission?

The delta between `processed_at` and `confirmed_at` is a strong real-time indicator of network congestion and fork stability.

Under healthy network conditions:
- The delta is typically **200–800ms** (a few slots), signaling rapid validator voting and fast consensus.
- Under high congestion, the delta stretches to **2–6 seconds** as vote propagation slows down across the cluster and fork churn increases.

Monitoring this delta in production allows the tracker to detect when cluster consensus is lagging, informing retry tip scaling and submission pacing decisions.

### Why should you never use `finalized` commitment when fetching a blockhash for a time-sensitive transaction?

Because `finalized` commitment lags significantly behind the tip of the chain (typically 31+ slots / ~12–15 seconds). A transaction signed with a `finalized` blockhash loses about 15 seconds of its ~60-second lifetime, increasing blockhash expiry risk.

Furthermore, we observed that using `confirmed` commitment can still lead to immediate expiry failures when querying lagged public RPC endpoints. If the public RPC load balancer routes the blockhash request to a lagging node but the block height check hits a caught-up node, the stack will instantly flag the blockhash as expired. 

To mitigate this, our stack fetches blockhashes using the **`processed` commitment level** (the absolute tip) and caches/throttles block height queries (polling at most once every 2 seconds) to guarantee consistency and maximize the validity window.

### What happens to your bundle if the Jito leader skips their slot?

If the Jito leader skips their slot, the bundle is **not landed** in that slot. Because bundles are tied to a specific leader's bundle stage, a skipped slot causes the block engine to discard the bundle.

Our system handles this by detecting missing commitment updates and executing an autonomous retry with a refreshed blockhash and adjusted tip for the next Jito leader window.

## Key Observations & Tradeoffs

- **RPC Load Balancer Lag:** Public RPC nodes (like the default mainnet endpoint) frequently return stale blockhashes. Fetching with `processed` commitment is crucial to prevent instant blockhash expiry.
- **Throttling RPC Calls:** Throttling block height checks to once every 2 seconds prevents rate-limiting issues on the RPC node while maintaining accurate tracking.
- **Tip Account Randomization:** Randomly distributing tips across all 8 Jito tip accounts prevents concentration of funds and adheres to Jito guidelines.
- **Simulation Mismatches & Preflight Bypassing:** Public RPC load balancers often route preflight simulation requests to nodes that haven't synchronized the latest blockhash, causing false `Blockhash not found` errors. Our standard RPC fallback bypasses this by intentionally using `skip_preflight: true`, shifting the simulation burden to the actual validator execution phase.
- **Fallback Resilience:** Dual-submitting to standard RPC endpoints alongside Jito guarantees that transactions land even when Jito block engines silently drop bundles or when Jito validators are sparse. Furthermore, deterministic retry fallbacks ensure that if the LLM API experiences rate limits, transactions are still recovered using a 1.5x tip multiplier.

## License

MIT
