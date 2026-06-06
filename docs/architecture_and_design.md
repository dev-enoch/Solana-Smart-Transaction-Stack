# Architecture & Design Document

> 📄 **Author:** Enoch Philip Dibal
> **Date:** June 2026
> **Bounty:** Solana Smart Transaction Infrastructure (Superteam Nigeria)
> **Repository:** https://github.com/dev-enoch/Solana-Smart-Transaction-Stack

## Executive Summary

This project delivers a production-grade, AI-augmented transaction infrastructure stack on Solana. It monitors the network in real time using Yellowstone gRPC, intelligently submits Jito bundles with dynamic tips, tracks the full transaction lifecycle across all commitment levels, and uses a reasoning AI agent to make autonomous operational decisions especially for failure recovery and retries.

The system is built in Rust for performance and reliability, with a clear separation between streaming, core transaction logic, lifecycle tracking, and the AI decision layer.

## System Architecture

### High-Level Overview

**Layers**

1. **Ingestion Layer:** Yellowstone gRPC streaming (slots, leader schedule, transaction updates)
2. **Core Engine:** Bundle construction, dynamic tip calculation, blockhash management
3. **Decision Layer:** AI Agent (autonomous reasoning)
4. **Tracking & Observability:** Full lifecycle logging + failure classification
5. **Submission Layer:** Jito Block Engine

**Data Flow Diagram** *(Insert Excalidraw / draw.io diagram here)*

- Yellowstone → Slot & Leader updates → Core decides optimal window
- Core fetches tip data → AI decides tip amount / timing
- Bundle built & submitted → Lifecycle tracker monitors commitments
- On failure → AI reasons → Decides retry strategy → Resubmit

## Key Components

| Component | Module | Main Responsibilities |
| --- | --- | --- |
| Yellowstone Streamer | `streaming/yellowstone.rs` | Live slot, leader schedule, tx status updates + reconnection |
| Tip Manager | `core/tip.rs` | Fetch live Jito tip accounts, calculate dynamic tips |
| Bundle Builder | `core/bundle.rs` | Build & submit Jito bundles with tip instruction |
| Lifecycle Tracker | `core/tracker.rs` | Track commitments, latencies, classify failures |
| AI Agent | `ai/agent.rs` | Autonomous decision making with Chain-of-Thought reasoning |
| Logging | `logging/` | Generate verifiable lifecycle logs |

## Design Decisions & Tradeoffs

- **Rust + Tokio**: Chosen for high performance, safety, and excellent async support needed for gRPC streaming.
- **gRPC Streaming**: Preferred over polling for real-time observability (as required).
- **Dynamic Tips**: Calculated from recent tip accounts + congestion factor instead of hardcoded values.
- **AI Agent**: Uses structured prompting + JSON output to ensure visible, auditable reasoning (not simple if-else automation).
- **Failure Handling**: Exponential backoff + AI-driven decisions (refresh blockhash, adjust tip, wait for leader, etc.).

## Failure Handling Strategy

- Automatic gRPC reconnection
- Blockhash expiry detection → AI decides refresh + resubmit
- Common failure classification: expired_blockhash, low_tip, bundle_drop, compute_exceeded, leader_skip
- Fault injection implemented for blockhash expiry to demonstrate autonomous retry

## AI Agent Responsibilities

The AI Agent owns **Failure Reasoning + Autonomous Retry**:

- Observes failure context (type, latency, slot, tip, etc.)
- Performs step-by-step reasoning
- Decides optimal next action
- Logs full reasoning trace for transparency

This satisfies the requirement for a meaningful, non-hardcoded operational decision.

## Infrastructure & Environment

- **Devnet + Mainnet** ready (configurable via .env)
- Uses public/high-performance Yellowstone endpoints + Jito Block Engine
- Proper backpressure and reconnection logic

## Future Improvements

- Multi-bundle parallel submissions
- Advanced congestion prediction using more Geyser data
- Leader schedule caching optimization

---

### Diagrams (Add 2–3)

1. High-Level Architecture
2. Transaction Lifecycle & AI Retry Loop
3. Component Data Flow

### Screenshots / Logs

- Sample lifecycle log entries
- AI reasoning examples
- Terminal output showing slot streaming + bundle submission
