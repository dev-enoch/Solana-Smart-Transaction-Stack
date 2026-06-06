use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{error, info};
use std::time::Duration;

use tonic::{Request, Status};
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Endpoint;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeUpdate,
};

use crate::types::streaming::{SlotUpdate, StreamEvent, TransactionUpdate};
use solana_client::rpc_client::RpcClient;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, EncodeBuf};
use yellowstone_grpc_proto::prost::Message;

#[derive(Default)]
pub struct YellowstoneCodec;

impl Codec for YellowstoneCodec {
    type Encode = SubscribeRequest;
    type Decode = SubscribeUpdate;

    type Encoder = YellowstoneEncoder;
    type Decoder = YellowstoneDecoder;

    fn encoder(&mut self) -> Self::Encoder { YellowstoneEncoder }
    fn decoder(&mut self) -> Self::Decoder { YellowstoneDecoder }
}

pub struct YellowstoneEncoder;
impl tonic::codec::Encoder for YellowstoneEncoder {
    type Item = SubscribeRequest;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        item.encode(buf).map_err(|e| Status::internal(format!("encode error: {}", e)))?;
        Ok(())
    }
}

pub struct YellowstoneDecoder;
impl tonic::codec::Decoder for YellowstoneDecoder {
    type Item = SubscribeUpdate;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let item = SubscribeUpdate::decode(buf).map_err(|e| Status::internal(format!("decode error: {}", e)))?;
        Ok(Some(item))
    }
}

pub struct YellowstoneStreamer {
    endpoint: String,
    x_token: Option<String>,
    event_tx: mpsc::Sender<StreamEvent>,
    rpc_client: Arc<RpcClient>,
    leader_schedule: Arc<RwLock<HashMap<u64, String>>>,
    payer_pubkey: String,
}

impl YellowstoneStreamer {
    pub async fn new(
        endpoint: &str,
        x_token: Option<String>,
        event_tx: tokio::sync::mpsc::Sender<StreamEvent>,
        rpc_url: &str,
        payer_pubkey: String,
    ) -> Result<Self> {
        let rpc_client = Arc::new(solana_client::rpc_client::RpcClient::new(rpc_url.to_string()));
        Ok(Self {
            endpoint: endpoint.to_string(),
            x_token,
            event_tx,
            rpc_client,
            leader_schedule: Arc::new(RwLock::new(HashMap::new())),
            payer_pubkey,
        })
    }

    pub async fn update_leader_schedule(&self, start_slot: u64) -> Result<()> {
        let leaders = self.rpc_client.get_slot_leaders(start_slot, 100)?;
        let mut schedule = self.leader_schedule.write().await;
        for (i, leader) in leaders.iter().enumerate() {
            schedule.insert(start_slot + i as u64, leader.to_string());
        }
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let uri = self.endpoint.parse::<tonic::transport::Uri>()?;
        let channel = Endpoint::from(uri.clone())
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await?;

        let token = self.x_token.clone().unwrap_or_default();
        
        let mut grpc_client = Grpc::new(channel);
        let mut backoff = Duration::from_secs(1);
        
        loop {
            let req = SubscribeRequest {
                slots: [("client".to_string(), SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(true),
                    ..Default::default()
                })].into_iter().collect(),
                transactions: [("txs".to_string(), SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: Some(true),
                    signature: None,
                    account_include: vec![self.payer_pubkey.clone()],
                    account_exclude: vec![],
                    account_required: vec![],
                })].into_iter().collect(),
                ..Default::default()
            };

            let req_stream = async_stream::stream! {
                yield req;
            };

            let mut request = Request::new(req_stream);
            if !token.is_empty() {
                if let Ok(metadata_val) = token.parse::<AsciiMetadataValue>() {
                    request.metadata_mut().insert("x-token", metadata_val);
                }
            }

            match grpc_client.ready().await {
                Ok(_) => {},
                Err(e) => {
                    error!("gRPC client ready failed: {}", e);
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            }

            let codec = YellowstoneCodec::default();
            let path = tonic::codegen::http::uri::PathAndQuery::from_static("/geyser.Geyser/Subscribe");
            
            let response = match grpc_client.streaming(request, path, codec).await {
                Ok(r) => r,
                Err(e) => {
                    error!("Yellowstone subscription error: {}", e);
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(30));
                    continue;
                }
            };
            
            backoff = Duration::from_secs(1);
            info!("Successfully connected to Yellowstone gRPC stream");
            
            let mut stream = response.into_inner();

            while let Ok(Some(message)) = stream.message().await {
                if let Some(update_oneof) = message.update_oneof {
                    match update_oneof {
                        UpdateOneof::Slot(slot) => {
                            let current_slot = slot.slot;
                            if current_slot % 50 == 0 {
                                let _ = self.update_leader_schedule(current_slot).await;
                            }

                            let schedule = self.leader_schedule.read().await;
                            let leader = schedule.get(&current_slot).cloned();

                            if let Err(e) = self.event_tx.send(StreamEvent::Slot(SlotUpdate {
                                slot: current_slot,
                                timestamp: chrono::Utc::now(),
                                leader,
                            })).await {
                                error!("Failed to send slot update: {:?}", e);
                                break;
                            }
                        }
                        UpdateOneof::Transaction(tx) => {
                            if let Some(info) = tx.transaction {
                                let sig_bytes = info.signature;
                                let signature = bs58::encode(&sig_bytes).into_string();
                                
                                let error = info.meta.and_then(|m| m.err.map(|e| format!("{:?}", e.err)));
                                
                                if let Err(e) = self.event_tx.send(StreamEvent::Transaction(TransactionUpdate {
                                    signature,
                                    slot: tx.slot,
                                    error,
                                })).await {
                                    error!("Failed to send tx update: {:?}", e);
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            error!("Yellowstone stream disconnected. Reconnecting in {:?}", backoff);
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, Duration::from_secs(30));
        }
    }

    pub fn is_optimal_submission_window(&self, _current_slot: u64, _target_leader: &str) -> bool {
        false
    }

    pub fn get_next_leader_window(&self) -> Option<u64> {
        None
    }
}
