use chrono::{DateTime, Utc};

/// A slot update received from the Yellowstone gRPC stream.
#[derive(Debug, Clone)]
pub struct SlotUpdate {
    pub slot: u64,
    pub timestamp: DateTime<Utc>,
    pub leader: Option<String>,
    /// Commitment level: 0 = Processed, 1 = Confirmed, 2 = Finalized, 3 = First shred received.
    pub status: i32,
    /// Current block height at this slot, used for proper blockhash expiry detection.
    pub block_height: Option<u64>,
}

/// A transaction update received from the Yellowstone gRPC stream.
#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub signature: String,
    pub slot: u64,
    pub error: Option<String>,
}

/// Events emitted by the Yellowstone streamer to the orchestrator.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Slot(SlotUpdate),
    Transaction(TransactionUpdate),
}

/// Health metrics for the Yellowstone gRPC stream.
#[derive(Debug, Clone, Default)]
pub struct StreamHealthMetrics {
    /// Timestamp of the last message received from the stream.
    pub last_message_at: Option<DateTime<Utc>>,
    /// Total number of reconnections since startup.
    pub reconnection_count: u64,
    /// Messages received in the current measurement window.
    pub messages_in_window: u64,
    /// Start of the current measurement window.
    pub window_start: Option<DateTime<Utc>>,
    /// Total slot updates received.
    pub total_slot_updates: u64,
    /// Total transaction updates received.
    pub total_tx_updates: u64,
    /// Total messages dropped due to backpressure.
    pub messages_dropped: u64,
}
