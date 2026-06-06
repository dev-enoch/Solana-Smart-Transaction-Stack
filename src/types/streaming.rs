use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct SlotUpdate {
    pub slot: u64,
    pub timestamp: DateTime<Utc>,
    pub leader: Option<String>, // pubkey
}

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub signature: String,
    pub slot: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Slot(SlotUpdate),
    Transaction(TransactionUpdate),
}
