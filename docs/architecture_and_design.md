# Architecture and Design Document

> **Author:** Enoch Philip Dibal
> **Date:** June 2026
> **Bounty:** Solana Smart Transaction Infrastructure (Superteam Nigeria)
> **Repository:** https://github.com/dev-enoch/Solana-Smart-Transaction-Stack

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Architecture Overview](#system-architecture-overview)
3. [Project Structure](#project-structure)
4. [Component Deep Dives](#component-deep-dives)
   - [Yellowstone Streamer](#1-yellowstone-streamer)
   - [Tip Manager](#2-tip-manager)
   - [Bundle Builder](#3-bundle-builder)
   - [Lifecycle Tracker](#4-lifecycle-tracker)
   - [AI Agent](#5-ai-agent)
   - [Memo Transaction Builder](#6-memo-transaction-builder)
   - [Orchestrator (main.rs)](#7-orchestrator)
5. [Data Types and Contracts](#data-types-and-contracts)
6. [Configuration and Environment](#configuration-and-environment)
7. [Failure Handling Strategy](#failure-handling-strategy)
8. [Design Decisions and Tradeoffs](#design-decisions-and-tradeoffs)
9. [Data Flow Diagrams](#data-flow-diagrams)
10. [Future Improvements](#future-improvements)

---

## Executive Summary

This project delivers a production-grade, AI-augmented transaction infrastructure stack on the Solana blockchain. It monitors the network in real time using Yellowstone gRPC (Geyser), intelligently submits Jito bundles with dynamic tips, tracks the full transaction lifecycle across all commitment levels, and uses a reasoning AI agent to make autonomous operational decisions, particularly for failure recovery and retries.

The system is built entirely in Rust for performance and reliability, with a clear separation between the following five layers:

1. **Ingestion Layer** (Yellowstone gRPC streaming for slots, leaders, and transaction updates)
2. **Core Engine** (Bundle construction, dynamic tip calculation, blockhash management, memo transactions)
3. **Decision Layer** (AI Agent with Chain-of-Thought reasoning)
4. **Tracking and Observability** (Full lifecycle logging with failure classification)
5. **Submission Layer** (Jito Block Engine via JSON-RPC)

---

## System Architecture Overview

The stack operates as a single-binary async Rust application powered by the Tokio runtime. All components communicate through in-process message passing using `tokio::sync::mpsc` channels and shared state protected by `Arc<RwLock>` and `Arc<Mutex>`.

### Architectural Layers

```
+------------------------------------------------------------------+
|                        ORCHESTRATOR (main.rs)                    |
|  Intent Queue | Event Loop | Leader-Aware Submission Scheduling  |
+------------------------------------------------------------------+
        |                  |                    |
        v                  v                    v
+----------------+  +----------------+  +------------------+
| INGESTION      |  | CORE ENGINE    |  | DECISION LAYER   |
| Yellowstone    |  | Bundle Builder |  | AI Agent         |
| gRPC Streamer  |  | Tip Manager    |  | (LLM-powered     |
| Leader Schedule|  | Memo Builder   |  |  failure reasoning|
+----------------+  +----------------+  |  and retry logic) |
        |                  |            +------------------+
        v                  v                    |
+------------------------------------------------------------------+
|                   TRACKING AND OBSERVABILITY                     |
|  Lifecycle Tracker | Commitment Monitoring | JSON Log Export     |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|                       SUBMISSION LAYER                           |
|            Jito Block Engine (sendBundle JSON-RPC)               |
+------------------------------------------------------------------+
```

---

## Project Structure

The project follows a modular Rust workspace layout. Each functional area is isolated into its own module for clarity and maintainability.

```
solana-smart-tx-stack-rs/
|
+-- Cargo.toml                    # Dependency manifest
+-- .env                          # Runtime configuration (secrets, endpoints)
+-- .env.example                  # Template for environment configuration
+-- lifecycle_logs.json           # Output: persisted lifecycle tracking data
+-- README.md                     # Repository overview and setup instructions
+-- docs/
|   +-- architecture_and_design.md   # This document
|
+-- src/
    +-- main.rs                   # Orchestrator: event loop, intent queue, submission logic
    +-- lib.rs                    # Library root: re-exports all public modules
    |
    +-- ai/
    |   +-- mod.rs                # Module declaration for the AI subsystem
    |   +-- agent.rs              # AiAgent: LLM-powered failure reasoning and decision making
    |
    +-- core/
    |   +-- mod.rs                # Module declaration for core subsystem
    |   +-- bundle.rs             # BundleBuilder: constructs and submits Jito bundles
    |   +-- tip.rs                # TipManager: dynamic tip calculation from on-chain fee data
    |   +-- tracker.rs            # LifecycleTracker: commitment tracking and expiry detection
    |   +-- memo.rs               # Memo v2 transaction builder for bundle payloads
    |
    +-- streaming/
    |   +-- mod.rs                # Module declaration for the streaming subsystem
    |   +-- yellowstone.rs        # YellowstoneStreamer: gRPC subscription, leader schedule, reconnection
    |
    +-- types/
    |   +-- mod.rs                # Module declaration for shared data types
    |   +-- ai.rs                 # FailureContext and AgentDecision structs
    |   +-- lifecycle.rs          # LifecycleEntry struct with full commitment tracking fields
    |   +-- streaming.rs          # SlotUpdate, TransactionUpdate, and StreamEvent enum
    |
    +-- logging/
        +-- mod.rs                # Reserved module for future structured logging extensions
```

### Dependency Overview

| Dependency | Version | Purpose |
| --- | --- | --- |
| `solana-sdk` | ~1.18 | Solana transaction primitives, keypairs, signing |
| `solana-client` | ~1.18 | RPC client for blockhash, leader schedule, fee queries |
| `solana-program` | ~1.18 | On-chain program types |
| `spl-memo` | 4.0.0 | SPL Memo program integration (build helper) |
| `yellowstone-grpc-proto` | 11 | Protobuf types for Yellowstone Geyser gRPC |
| `tonic` | 0.10.2 | gRPC client with TLS support |
| `tokio` | 1 (full) | Async runtime |
| `reqwest` | 0.12 | HTTP client for Jito Block Engine and LLM API |
| `serde` / `serde_json` | 1.0 | JSON serialization and deserialization |
| `chrono` | 0.4 | Timestamps with serde support |
| `tracing` / `tracing-subscriber` | 0.1 / 0.3 | Structured logging |
| `dotenv` | 0.15 | Environment variable loading from `.env` |
| `bs58` | 0.5.1 | Base58 encoding for Solana signatures and transactions |
| `bincode` | 1.3 | Binary serialization for Solana transactions |
| `async-stream` | 0.3.6 | Async stream construction for gRPC request streams |

---

## Component Deep Dives

Each component below is documented with its exact file location, struct fields, public API, internal behavior, and how it connects to other components.

---

### 1. Yellowstone Streamer

**File:** `src/streaming/yellowstone.rs`
**Struct:** `YellowstoneStreamer`

#### Purpose

The Yellowstone Streamer is the ingestion layer of the stack. It establishes a persistent gRPC connection to a Yellowstone (Geyser) endpoint and subscribes to two categories of real-time updates:

- **Slot updates** (filtered by commitment level)
- **Transaction updates** (filtered to only include transactions involving the payer's public key)

It also maintains a local leader schedule cache and provides methods for determining whether the current slot falls within an optimal Jito submission window.

#### Struct Fields

| Field | Type | Description |
| --- | --- | --- |
| `endpoint` | `String` | The Yellowstone gRPC endpoint URL |
| `x_token` | `Option<String>` | Optional authentication token for gRPC metadata |
| `event_tx` | `mpsc::Sender<StreamEvent>` | Channel sender for pushing events to the orchestrator |
| `rpc_client` | `Arc<RpcClient>` | Solana RPC client used to fetch leader schedules |
| `leader_schedule` | `Arc<RwLock<HashMap<u64, String>>>` | Cached mapping from slot number to leader validator pubkey |
| `payer_pubkey` | `String` | The payer's public key, used to filter the transaction subscription |
| `jito_validators` | `HashSet<String>` | Set of known Jito validator public keys for leader targeting |

#### Key Methods

**`new(endpoint, x_token, event_tx, rpc_url, payer_pubkey, jito_validators) -> Result<Self>`**

Constructs a new streamer instance. Initializes the internal RPC client and creates an empty leader schedule cache.

**`start(&mut self) -> Result<()>`**

The main streaming loop. This method:

1. Parses the endpoint URI and establishes a TLS-enabled gRPC channel with a 10-second connection timeout.
2. Constructs a `SubscribeRequest` with two filters:
   - A **slot filter** named `"client"` with `filter_by_commitment: true`.
   - A **transaction filter** named `"txs"` with `vote: false`, `failed: true`, and `account_include` set to the payer's pubkey. This ensures the stream only receives transactions that involve the payer's wallet, keeping bandwidth costs minimal.
3. Attaches the `x-token` header to the request metadata if an authentication token is configured.
4. Enters a reconnection loop:
   - Waits for the gRPC client to be ready.
   - Opens a bidirectional streaming RPC call to `/geyser.Geyser/Subscribe`.
   - Processes incoming messages, dispatching `SlotUpdate` and `TransactionUpdate` events through the `event_tx` channel.
   - On disconnection, performs exponential backoff (starting at 1 second, capped at 30 seconds).
5. Every 50 slots, refreshes the leader schedule by calling `update_leader_schedule`.

**`update_leader_schedule(&self, start_slot: u64) -> Result<()>`**

Fetches the next 100 leaders from the Solana RPC (`get_slot_leaders`) and updates the internal `leader_schedule` cache.

**`is_optimal_submission_window(&self, current_slot: u64) -> bool`**

Looks ahead 4 slots from the current slot. Returns `true` if any of those upcoming leaders is a known Jito validator (present in the `jito_validators` set). This is used by the orchestrator to decide whether to submit a bundle now or wait.

**`get_next_leader_window(&self, current_slot: u64) -> Option<u64>`**

Scans up to 400 slots ahead to find the next slot where a Jito validator is the leader. Returns `Some(slot)` if found, or `None` if no Jito leader is scheduled within the lookahead window.

#### Custom Codec

The streamer uses a custom `YellowstoneCodec` implementation (`YellowstoneEncoder` and `YellowstoneDecoder`) to handle protobuf encoding and decoding of `SubscribeRequest` and `SubscribeUpdate` messages over the raw `tonic::client::Grpc` interface. This approach was chosen instead of the generated client stubs to avoid dependency conflicts with the `yellowstone-grpc-proto` crate.

#### Reconnection Strategy

| Scenario | Behavior |
| --- | --- |
| gRPC client not ready | Log error, sleep for current backoff, retry |
| Subscription error | Log error, sleep with exponential backoff (1s to 30s), retry |
| Stream disconnected | Log error, sleep with exponential backoff (1s to 30s), retry |
| Successful reconnection | Reset backoff to 1 second |

---

### 2. Tip Manager

**File:** `src/core/tip.rs`
**Struct:** `TipManager`

#### Purpose

The Tip Manager is responsible for providing dynamic, network-aware tip amounts for Jito bundles. Rather than using hardcoded tip values, it periodically polls the Solana RPC for recent prioritization fee data and uses that data as a baseline for calculating competitive tips.

#### Struct Fields

| Field | Type | Description |
| --- | --- | --- |
| `_rpc_client` | `Arc<RpcClient>` | Solana RPC client for querying prioritization fees |
| `recent_tips` | `Arc<RwLock<Vec<u64>>>` | Cached list of recent top prioritization fees (in lamports) |

#### Key Methods

**`new(rpc_url: &str) -> Self`**

Constructs a new Tip Manager with an empty recent tips cache.

**`get_tip_accounts(&self) -> Result<Vec<Pubkey>>`**

Returns the list of 8 known Jito tip accounts. These are the standard Jito tip distribution accounts that bundles must tip in order to be processed by the Block Engine:

- `96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5`
- `HFqU5xCUcjZk5hGcLcGm119Cjg5vK5aB6E6H2rJq22A2`
- `Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY`
- `ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iMgaSbg`
- `DfXygSm4jMyxPMAxVnLpsT4B32y8Zw4gqM9RMBH7E5vX`
- `3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBn1rvwB9x`
- `EMDUBo7iUjRCR9GtoG34ZADG2BwQ38o9yqRz6F2vJvT1`
- `DWY2zM19vCokwE4q4qjT37XEXgGqjX318QhT3eTqU1dJ`

**`calculate_dynamic_tip(&self, base_lamports: u64, congestion_factor: f64) -> Result<u64>`**

Calculates a dynamic tip using the following algorithm:

1. Read the cached `recent_tips`.
2. If no recent tips are available, fall back to `base_lamports`.
3. Otherwise, compute the arithmetic average of the cached tips.
4. Multiply the average by the `congestion_factor`.
5. Ensure the result is at least as large as `base_lamports` (floor enforcement).
6. Return the computed tip in lamports.

**`start_tip_updater(&self)`**

Spawns a background Tokio task that runs on a 10-second interval. On each tick, it:

1. Calls `get_recent_prioritization_fees(&[])` on the Solana RPC.
2. Sorts the returned fees in descending order.
3. Takes the top 20 non-zero fees.
4. Updates the shared `recent_tips` cache.

If the RPC call fails, the error is logged and the previous cache is retained.

---

### 3. Bundle Builder

**File:** `src/core/bundle.rs`
**Struct:** `BundleBuilder`

#### Purpose

The Bundle Builder is responsible for constructing and submitting transaction bundles to the Jito Block Engine. It takes a list of pre-built `VersionedTransaction` objects, appends a tip transaction, serializes the bundle, and submits it via the Jito `sendBundle` JSON-RPC endpoint.

#### Struct Fields

| Field | Type | Description |
| --- | --- | --- |
| `jito_url` | `String` | The base URL of the Jito Block Engine |
| `payer` | `Arc<Keypair>` | The payer keypair used for signing tip transactions |
| `tip_manager` | `TipManager` | Reference to the Tip Manager for dynamic tip calculation |
| `rpc_client` | `Arc<RpcClient>` | Solana RPC client for fetching the latest blockhash |
| `http_client` | `Client` | Reqwest HTTP client for submitting bundles to Jito |

#### Key Method: `build_and_submit`

**Signature:** `async fn build_and_submit(&self, mut transactions: Vec<VersionedTransaction>, slot: u64) -> Result<(String, Vec<String>, u64)>`

**Returns:** A tuple of `(bundle_id, signatures, last_valid_block_height)`.

**Step-by-step execution:**

1. **Fetch a tip account** from the `TipManager`. Uses the first account from the list.
2. **Calculate the dynamic tip** by calling `calculate_dynamic_tip(10_000, 1.5)` with a base of 10,000 lamports and a 1.5x congestion factor.
3. **Fetch a fresh blockhash** from the RPC using `confirmed` commitment level. This also retrieves `last_valid_block_height` for expiry tracking.
4. **Build the tip transaction** as a `system_instruction::transfer` from the payer to the selected tip account.
5. **Sign and append** the tip transaction to the end of the bundle's transaction list.
6. **Serialize each transaction** using `bincode`, then encode to Base58.
7. **Construct the JSON-RPC payload** using the `sendBundle` method.
8. **POST to the Jito Block Engine** at `{jito_url}/api/v1/bundles`.
9. **Parse the response**, extracting the `bundle_id` from the `result` field.
10. **Return** the bundle ID, all transaction signatures, and the `last_valid_block_height` for lifecycle tracking.

#### Error Handling

- Non-2xx HTTP responses from Jito are surfaced as errors with the full response body.
- JSON-RPC error responses (containing an `"error"` field) are caught and returned as `anyhow` errors.

---

### 4. Lifecycle Tracker

**File:** `src/core/tracker.rs`
**Struct:** `LifecycleTracker`

#### Purpose

The Lifecycle Tracker maintains an in-memory registry of all submitted bundles and monitors their progression through the Solana commitment levels: `pending` to `processed` to `confirmed` to `finalized`. It also detects blockhash expiry and records failures.

#### Struct Fields

| Field | Type | Description |
| --- | --- | --- |
| `entries` | `Arc<RwLock<HashMap<String, LifecycleEntry>>>` | Map from `bundle_id` to its full lifecycle entry |
| `sig_to_bundle` | `Arc<RwLock<HashMap<String, String>>>` | Reverse lookup from transaction signature to `bundle_id` |
| `log_file` | `String` | File path for persisted JSON lifecycle logs |

#### Key Methods

**`record_submission(bundle_id, slot, tip, signatures, last_valid_block_height)`**

Records a new bundle submission. Creates a `LifecycleEntry` with status `"pending"` and populates the signature-to-bundle reverse index.

**`update_status(bundle_id, commitment, slot)`**

Updates the commitment status of a bundle. Depending on the `commitment` parameter:

- `"processed"`: Sets `processed_at`, `processed_slot`, calculates `latency_processed_ms`.
- `"confirmed"`: Sets `confirmed_at`, `confirmed_slot`, calculates `latency_confirmed_ms`.
- `"finalized"`: Sets `finalized_at`, `finalized_slot`.

Latencies are computed as the elapsed time from the original `submitted_at` timestamp.

**`update_status_by_sig(signature, commitment, slot)`**

Convenience method that resolves a transaction signature to its bundle ID using the reverse index, then delegates to `update_status`.

**`record_failure(bundle_id, failure_type)`**

Marks a bundle as `"failed"` and records the failure classification string.

**`check_expiries(current_slot) -> Vec<(String, u64, u64)>`**

Iterates through all entries with status `"pending"`. For each entry that has a `last_valid_block_height` and where the `current_slot` exceeds that height, the entry is marked as failed with `failure_type: "expired_blockhash"`. Returns a list of `(bundle_id, slot_submitted, tip_lamports)` tuples for the orchestrator to pass to the AI Agent.

**`save_logs()`**

Serializes all lifecycle entries to pretty-printed JSON and writes them to the configured `log_file` (default: `lifecycle_logs.json`). This runs every 5 seconds via a background Tokio task spawned by the orchestrator.

---

### 5. AI Agent

**File:** `src/ai/agent.rs`
**Struct:** `AiAgent`

#### Purpose

The AI Agent is the decision-making brain of the stack. When a bundle failure is detected, the AI Agent receives full failure context, constructs a structured prompt, calls an external LLM API, and returns a reasoned decision about how to handle the failure. This is not hardcoded if-else logic; the agent performs genuine Chain-of-Thought reasoning through the LLM.

#### Struct Fields

| Field | Type | Description |
| --- | --- | --- |
| `client` | `Client` | Reqwest HTTP client for LLM API calls |
| `api_url` | `String` | The LLM API endpoint URL |
| `api_key` | `String` | Authentication key for the LLM API |

#### Key Methods

**`decide_on_failure(failure_context: FailureContext) -> Result<AgentDecision>`**

The primary entry point. This method:

1. Builds a structured reasoning prompt using `build_failure_reasoning_prompt`.
2. Calls the LLM via `call_llm`.
3. If the LLM call fails, returns an error immediately (no fallback simulation). This ensures the system never produces fabricated reasoning.
4. Parses the LLM's JSON response into an `AgentDecision` struct.
5. Logs the reasoning and action.

**`build_failure_reasoning_prompt(ctx: &FailureContext) -> String`**

Constructs a detailed prompt that provides the LLM with:

- The bundle ID, failure type, slot number, tip amount, and latency
- Additional context details from the `extra` field
- A step-by-step reasoning instruction
- Five possible actions to choose from: retry with same tip, retry with higher tip, refresh blockhash and retry, wait for better leader slot, or abort
- A strict JSON output schema specifying the expected fields: `reasoning`, `root_cause`, `action`, `new_tip_lamports`, and `wait_slots`

**`call_llm(prompt: &str) -> Result<String>`**

Sends the prompt to the configured LLM API. The implementation:

1. Constructs a JSON payload following the Google Generative AI format (with `contents`, `systemInstruction`, and `generationConfig` fields).
2. Sets `temperature: 0.0` for deterministic output.
3. Sets `responseMimeType: "application/json"` to request structured JSON output.
4. Appends the API key as a query parameter.
5. Sends the POST request and validates the response status.
6. Extracts the generated text from `candidates[0].content.parts[0].text`.
7. Strips any markdown code block wrappers (such as triple backtick json fences) that the LLM may include.
8. Returns the cleaned JSON string.

#### Error Handling Philosophy

When the LLM API is unreachable or returns an error, the agent **does not fall back to a simulated response**. Instead, it propagates the error upward. This design choice ensures that the system never presents fabricated AI reasoning as genuine, which would be misleading to operators and judges.

---

### 6. Memo Transaction Builder

**File:** `src/core/memo.rs`
**Function:** `create_memo_tx`

#### Purpose

The Memo Transaction Builder creates Versioned Transactions (v0) that write a text message to the Solana Memo Program v2. Each bundle submission includes a memo transaction containing a human-readable string that embeds the bundle number and submission slot for easy on-chain verification.

#### Implementation Details

**Memo Program ID (v2):** `MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr`

**Function Signature:**
```rust
pub fn create_memo_tx(
    payer: &Keypair,
    message: &str,
    recent_blockhash: &Hash,
    tip_ix: Option<Instruction>,
) -> VersionedTransaction
```

**Behavior:**

1. Creates a memo instruction by calling `Instruction::new_with_bytes` with the Memo v2 program ID, the message bytes as data, and the payer as a signer account.
2. If an optional `tip_ix` instruction is provided, appends it to the instruction list. This allows a single transaction to carry both the memo and the Jito tip.
3. Compiles the instructions into a v0 `Message` using `Message::try_compile` with an empty address lookup table list.
4. Signs and returns a `VersionedTransaction`.

#### Memo Format

Each memo follows the pattern: `"smart-stack test bundle #N slot S"` where `N` is the bundle number and `S` is the current slot at the time of submission.

---

### 7. Orchestrator

**File:** `src/main.rs`

#### Purpose

The orchestrator is the central coordinator of the entire stack. It initializes all components, manages the intent queue, runs the main event loop, and coordinates the flow of data between the streamer, the bundle builder, the lifecycle tracker, and the AI agent.

#### Initialization Sequence

1. Load environment variables from `.env` using `dotenv`.
2. Initialize `tracing_subscriber` for structured logging.
3. Read all configuration values from environment variables (see [Configuration and Environment](#configuration-and-environment)).
4. Parse the `PRIVATE_KEY` from a JSON byte array and construct the payer `Keypair`.
5. Instantiate the following components:
   - `RpcClient` (wrapped in `Arc`)
   - `TipManager`
   - `BundleBuilder` (wrapped in `Arc`)
   - `LifecycleTracker` (wrapped in `Arc`, configured with `lifecycle_logs.json`)
   - `AiAgent` (wrapped in `Arc`)
6. Start the Tip Manager's background updater task via `start_tip_updater()`.
7. Create the `mpsc::channel` for `StreamEvent` messages with a buffer size of 100.
8. Instantiate the `YellowstoneStreamer` and spawn it as a background task.
9. Populate the intent queue with 3 test intents (the 2nd one has fault injection enabled).
10. Spawn the lifecycle log persistence task (saves every 5 seconds).
11. Enter the main event loop.

#### Intent Queue

The intent queue (`Arc<Mutex<Vec<Intent>>>`) holds a list of pending transaction intents. Each `Intent` struct contains:

| Field | Type | Description |
| --- | --- | --- |
| `id` | `String` | Unique identifier for the intent |
| `memo` | `String` | The memo text to embed in the transaction |
| `retries` | `u32` | Number of times this intent has been retried |
| `override_tip` | `Option<u64>` | If set, overrides the default tip with this value |
| `fault_inject_bad_blockhash` | `bool` | If `true`, replaces the blockhash with `Hash::default()` to simulate expiry |
| `target_slot` | `Option<u64>` | If set, the intent will only be submitted at or after this slot |

#### Main Event Loop

The event loop processes `StreamEvent` messages from the Yellowstone Streamer channel:

**On `StreamEvent::Slot(update)`:**

1. Record the `current_slot`.
2. Check for expired bundles via `tracker.check_expiries(current_slot)`.
3. For each expired bundle, construct a `FailureContext` and spawn an async task that:
   - Calls `ai_agent.decide_on_failure(ctx)`.
   - Matches the AI's decision and acts accordingly:
     - `"refresh_blockhash"`: Creates a new intent with refreshed blockhash settings and the AI's suggested tip.
     - `"retry_higher_tip"`: Creates a new intent with the AI's recommended higher tip amount.
     - `"wait"`: Creates a new intent with a `target_slot` set to `current_slot + wait_slots`.
     - `"abort"` (or any unknown action): Drops the intent entirely with a log message.
4. Check the intent queue. If there are pending intents:
   - Call `streamer.is_optimal_submission_window(current_slot)` to check if a Jito validator is the leader in the next 4 slots.
   - If the first intent has a `target_slot`, also verify that `current_slot >= target_slot`.
   - If both conditions are met, dequeue the intent and submit it:
     - Fetch a fresh blockhash (or inject a bad one if fault injection is enabled).
     - Build a memo transaction using `create_memo_tx` with the format `"{memo} slot {current_slot}"`.
     - Spawn an async task to call `bundle_builder.build_and_submit(...)`.
     - On success, record the submission in the lifecycle tracker.

**On `StreamEvent::Transaction(tx_update)`:**

1. Check the `error` field of the transaction update.
2. If an error is present, update the status to `"failed"`.
3. If no error, update the status to `"processed"`.

#### Concurrency Model

The orchestrator uses `tokio::spawn` extensively to avoid blocking the main event loop. The following tasks run concurrently:

| Task | Lifetime | Description |
| --- | --- | --- |
| Yellowstone Streamer | Permanent | Maintains the gRPC connection and pushes events |
| Tip Updater | Permanent | Polls RPC for prioritization fees every 10 seconds |
| Log Persistence | Permanent | Writes lifecycle entries to JSON every 5 seconds |
| Bundle Submission | Per-intent | Submits a single bundle and records the result |
| AI Failure Handling | Per-failure | Calls the LLM and enqueues a retry intent |

---

## Data Types and Contracts

All shared data types are defined in the `src/types/` module.

### StreamEvent (types/streaming.rs)

```rust
pub enum StreamEvent {
    Slot(SlotUpdate),
    Transaction(TransactionUpdate),
}
```

**`SlotUpdate`** contains `slot: u64`, `timestamp: DateTime<Utc>`, and `leader: Option<String>`.

**`TransactionUpdate`** contains `signature: String`, `slot: u64`, and `error: Option<String>`.

### LifecycleEntry (types/lifecycle.rs)

A comprehensive struct that tracks a bundle from submission through finalization:

| Field | Type | Description |
| --- | --- | --- |
| `bundle_id` | `String` | Unique bundle identifier returned by Jito |
| `slot_submitted` | `u64` | The slot at which the bundle was submitted |
| `submitted_at` | `DateTime<Utc>` | Timestamp of submission |
| `processed_at` | `Option<DateTime<Utc>>` | Timestamp when the transaction reached `processed` commitment |
| `processed_slot` | `Option<u64>` | Slot at which `processed` was observed |
| `confirmed_at` | `Option<DateTime<Utc>>` | Timestamp when `confirmed` commitment was reached |
| `confirmed_slot` | `Option<u64>` | Slot at which `confirmed` was observed |
| `finalized_at` | `Option<DateTime<Utc>>` | Timestamp when `finalized` commitment was reached |
| `finalized_slot` | `Option<u64>` | Slot at which `finalized` was observed |
| `tip_lamports` | `u64` | The tip paid for this bundle |
| `status` | `String` | Current status: `"pending"`, `"processed"`, `"confirmed"`, `"finalized"`, or `"failed"` |
| `failure_type` | `Option<String>` | If failed, the failure classification |
| `latency_processed_ms` | `Option<i64>` | Milliseconds from submission to `processed` |
| `latency_confirmed_ms` | `Option<i64>` | Milliseconds from submission to `confirmed` |
| `last_valid_block_height` | `Option<u64>` | The block height after which the blockhash expires |
| `signatures` | `Vec<String>` | All transaction signatures in the bundle |

The default status is `"pending"`.

### FailureContext (types/ai.rs)

```rust
pub struct FailureContext {
    pub bundle_id: String,
    pub failure_type: String,
    pub slot: u64,
    pub tip: u64,
    pub latency: i64,
    pub extra: String,
}
```

Passed to the AI Agent when a failure is detected. The `extra` field contains human-readable context about the failure circumstances.

### AgentDecision (types/ai.rs)

```rust
pub struct AgentDecision {
    pub reasoning: String,
    pub root_cause: String,
    pub action: String,
    pub new_tip_lamports: Option<u64>,
    pub wait_slots: Option<u64>,
}
```

Returned by the AI Agent. The `action` field must be one of: `"refresh_blockhash"`, `"retry_higher_tip"`, `"wait"`, or `"abort"`.

---

## Configuration and Environment

All runtime configuration is managed through environment variables loaded from the `.env` file at startup.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `YELLOWSTONE_ENDPOINT` | No | `http://localhost:50051` | Yellowstone gRPC endpoint URL |
| `YELLOWSTONE_X_TOKEN` | No | None | Authentication token for gRPC metadata header |
| `JITO_BLOCK_ENGINE_URL` | No | `https://amsterdam.mainnet.block-engine.jito.wtf` | Jito Block Engine base URL |
| `RPC_URL` | No | `https://api.devnet.solana.com` | Solana RPC endpoint for blockhash and fee queries |
| `JITO_VALIDATORS` | No | Empty | Comma-separated list of known Jito validator pubkeys |
| `AI_API_URL` | No | `https://api.x.ai/v1/chat/completions` | LLM API endpoint URL |
| `XAI_API_KEY` | No | Empty | API key for the LLM service |
| `PRIVATE_KEY` | **Yes** | None | JSON byte array of the payer's Solana keypair (panics if missing) |
| `PUBLIC_KEY` | No | Not used in code | Informational, not consumed by the application |

---

## Failure Handling Strategy

The stack implements a multi-layered approach to failure detection and recovery.

### Layer 1: Automatic gRPC Reconnection

The Yellowstone Streamer handles connection drops gracefully through exponential backoff, starting at 1 second and capping at 30 seconds. On successful reconnection, the backoff resets to 1 second. This prevents log flooding and excessive reconnection attempts during sustained outages.

### Layer 2: Blockhash Expiry Detection

The Lifecycle Tracker monitors all pending bundles against the current slot. When `current_slot` exceeds a bundle's `last_valid_block_height`, the bundle is marked as failed with `failure_type: "expired_blockhash"`. This triggers the AI Agent pipeline.

### Layer 3: AI-Driven Failure Recovery

When a failure is detected, the full context (bundle ID, failure type, slot, tip, latency, and additional details) is passed to the AI Agent. The agent produces one of four actions:

| Action | Behavior |
| --- | --- |
| `refresh_blockhash` | Create a new intent that will fetch a fresh blockhash before submission |
| `retry_higher_tip` | Create a new intent with the AI's recommended tip amount |
| `wait` | Create a new intent with a `target_slot` offset, delaying submission |
| `abort` | Drop the intent entirely, logging the decision |

### Layer 4: Fault Injection (Testing)

The second test intent is deliberately configured with `fault_inject_bad_blockhash: true`. This:

1. Replaces the real blockhash with `Hash::default()` (all zeros).
2. Artificially sets the `last_valid_block_height` to `current_slot + 5` so the blockhash expires almost immediately.

This allows end-to-end testing of the failure detection and AI retry pipeline without waiting for a natural blockhash expiry (which takes approximately 60 to 90 seconds).

### Failure Classifications

| Failure Type | Trigger | Description |
| --- | --- | --- |
| `expired_blockhash` | `current_slot > last_valid_block_height` | The transaction's blockhash expired before landing |
| `low_tip` | Reported by AI analysis | The tip was insufficient for the bundle to be prioritized |
| `bundle_drop` | No commitment after timeout | The bundle was dropped by the Block Engine |
| `compute_exceeded` | On-chain error | The transaction exceeded compute budget limits |
| `leader_skip` | No commitment + slot progression | The assigned leader missed their slot |

---

## Design Decisions and Tradeoffs

### Rust and Tokio

Rust was chosen for its combination of memory safety without garbage collection, zero-cost abstractions, and excellent async support through Tokio. The Solana ecosystem has first-class Rust support, and all critical dependencies (`solana-sdk`, `solana-client`, `yellowstone-grpc-proto`) are native Rust crates.

### gRPC Streaming Over Polling

The Yellowstone gRPC subscription provides sub-second slot updates without the overhead of repeated HTTP requests. This is essential for leader-aware submission timing, where a delay of even a few hundred milliseconds can mean missing the optimal Jito leader window.

### Dynamic Tips Over Hardcoded Values

Hardcoded tips fail during congestion (too low to compete) and waste funds during calm periods (overpaying). The dynamic tip calculation uses real fee data from the network as a baseline and applies a configurable congestion multiplier. The floor enforcement (`max(base_lamports, ...)`) prevents tips from dropping to zero during low-activity periods.

### AI Agent with Real LLM Calls (No Simulation)

The AI Agent calls a real LLM API for every failure decision. If the LLM is unreachable, the agent returns an error rather than falling back to hardcoded logic. This ensures:

1. The reasoning is genuine and auditable.
2. The system does not silently degrade to deterministic logic while appearing to use AI.
3. Judges can verify that the AI is making real, non-trivial decisions by inspecting the logged reasoning traces.

### Versioned Transactions (v0) with Memo Program v2

The stack uses Versioned Transaction format (v0) for memo transactions. This provides forward compatibility with Address Lookup Tables and aligns with modern Solana transaction standards. The Memo Program v2 (`MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr`) is the current standard and is widely supported by block explorers.

### Filtered gRPC Subscriptions for Cost Efficiency

The transaction filter uses `account_include: [payer_pubkey]` to restrict the stream to only transactions involving the payer's wallet. Without this filter, a full Yellowstone stream can consume 150 to 200 Mbps of bandwidth, costing over $100 per day on Pay-As-You-Go plans. With the filter, the estimated cost is less than $0.01 per day.

---

## Data Flow Diagrams

### High-Level Architecture Flow

```
                    +-------------------+
                    |   Solana Network  |
                    +-------------------+
                      |             ^
          gRPC Stream |             | sendBundle (JSON-RPC)
                      v             |
              +--------------+  +------------------+
              | Yellowstone  |  | Jito Block       |
              | Streamer     |  | Engine           |
              +--------------+  +------------------+
                      |             ^
            mpsc      |             | HTTP POST
            channel   v             |
              +--------------+  +------------------+
              | Orchestrator |->| Bundle Builder   |
              | (main.rs)    |  | + Tip Manager    |
              +--------------+  +------------------+
                |         |
                v         v
        +----------+  +----------+
        | Lifecycle |  | AI Agent |
        | Tracker   |  | (LLM)   |
        +----------+  +----------+
                |         |
                v         v
        +----------+  +----------+
        | JSON Log |  | Intent   |
        | File     |  | Queue    |
        +----------+  +----------+
```

### Transaction Lifecycle and AI Retry Loop

```
  Intent Created
       |
       v
  Wait for Optimal Slot (Jito Leader Window)
       |
       v
  Build Memo Tx (Memo v2)
       |
       v
  Build and Submit Bundle (with Dynamic Tip)
       |
       +-------> Record in Lifecycle Tracker (status: "pending")
       |
       v
  Stream Monitors for Commitment Updates
       |
       +----> Transaction Processed?
       |         |
       |    Yes: Update status to "processed"
       |    No:  Check if blockhash expired
       |              |
       |         Yes: Mark as "failed: expired_blockhash"
       |              |
       |              v
       |         Build FailureContext
       |              |
       |              v
       |         AI Agent: decide_on_failure()
       |              |
       |              +---> "refresh_blockhash" --> New Intent (queue)
       |              +---> "retry_higher_tip"  --> New Intent (queue)
       |              +---> "wait"              --> New Intent (with target_slot)
       |              +---> "abort"             --> Drop (log and discard)
       |
       +----> Transaction Confirmed?
       |         Update status to "confirmed"
       |
       +----> Transaction Finalized?
                 Update status to "finalized"
```

### Component Data Flow

```
  .env File
     |
     v
  main.rs (reads env vars)
     |
     +---> YellowstoneStreamer::new()
     |        |
     |        +---> gRPC connect to YELLOWSTONE_ENDPOINT
     |        +---> RPC client for leader schedule (RPC_URL)
     |        +---> Filter: account_include = [payer pubkey]
     |
     +---> TipManager::new()
     |        |
     |        +---> start_tip_updater() [10s interval]
     |                  |
     |                  +---> RPC: get_recent_prioritization_fees()
     |                  +---> Cache top 20 fees in Arc<RwLock<Vec>>
     |
     +---> BundleBuilder::new()
     |        |
     |        +---> Uses TipManager for dynamic tips
     |        +---> Uses RPC for blockhash (confirmed commitment)
     |        +---> HTTP POST to JITO_BLOCK_ENGINE_URL/api/v1/bundles
     |
     +---> LifecycleTracker::new()
     |        |
     |        +---> In-memory HashMap<bundle_id, LifecycleEntry>
     |        +---> save_logs() writes to lifecycle_logs.json [5s interval]
     |
     +---> AiAgent::new()
              |
              +---> HTTP POST to AI_API_URL with API key
              +---> Returns AgentDecision (JSON parsed)
```

---

## Future Improvements

1. **Multi-Bundle Parallel Submissions**: Currently, the orchestrator submits one intent at a time from the front of the queue. A future version could batch multiple intents into parallel bundle submissions for higher throughput.

2. **Advanced Congestion Prediction**: Leverage additional Geyser data (such as block times, vote account health, and skip rates) to build a predictive model for network congestion, allowing pre-emptive tip adjustments before failures occur.

3. **Leader Schedule Caching Optimization**: The current implementation refreshes the leader schedule every 50 slots by calling the RPC. A more efficient approach would be to subscribe to epoch boundary events and refresh only once per epoch.

4. **Commitment Level Subscriptions**: Extend the Yellowstone subscription to include `confirmed` and `finalized` commitment updates, enabling the tracker to progress bundles through all three commitment levels from the stream rather than relying solely on `processed` notifications.

5. **Retry Budget and Circuit Breaker**: Implement a maximum retry count and a cost-based circuit breaker to prevent runaway tip escalation during sustained network issues.

6. **Metrics and Dashboard**: Expose Prometheus-compatible metrics for bundle success rates, average latencies, tip distributions, and AI decision distributions. Pair with a Grafana dashboard for real-time operational visibility.

---

*This document was last updated on June 6, 2026.*
