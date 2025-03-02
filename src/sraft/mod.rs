use std::collections::HashMap;

use crate::config::Config;
use anyhow::Result;
use api::grpc;
use state_machine::{Message, StateMachine};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

pub mod api;
mod state_machine;

pub struct SraftNode {
    actions: mpsc::Sender<Message>,
}

impl SraftNode {
    pub fn new(cfg: &Config) -> Result<Self> {
        let (send, recv) = mpsc::channel(8);

        let mut sm = StateMachine::new(
            cfg.peer_id as usize,
            cfg.peers.iter().map(|x| x.addr.clone()).collect(),
            recv,
            send.clone(),
        )?;
        tokio::spawn(async move { sm.run().await });

        info!(
            peer_id = cfg.peer_id,
            addr = cfg.peers[cfg.peer_id as usize].addr,
            "staring peer"
        );

        Ok(SraftNode { actions: send })
    }

    async fn get(&self, key: String) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        let msg = Message::Get { key, resp: tx };
        let _ = self.actions.send(msg).await;
        rx.await?
    }

    async fn set(&self, key: String, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        let msg = Message::Set {
            key,
            value,
            resp: tx,
        };
        let _ = self.actions.send(msg).await;
        rx.await?
    }

    async fn request_vote(
        &self,
        req: grpc::RequestVoteRequest,
    ) -> Result<grpc::RequestVoteResponse> {
        let (tx, rx) = oneshot::channel();
        let msg = Message::RequestVote { req, resp: tx };
        let _ = self.actions.send(msg).await;
        rx.await?
    }

    async fn append_entries(
        &self,
        req: grpc::AppendEntriesRequest,
    ) -> Result<grpc::AppendEntriesResponse> {
        let (tx, rx) = oneshot::channel();
        let msg = Message::AppendEntries { req, resp: tx };
        let _ = self.actions.send(msg).await;
        rx.await?
    }
}
