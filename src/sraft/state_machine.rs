use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Result};
use rand::rng;
use rand::Rng;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;
use tokio::time::Instant;
use tokio::time::Sleep;
use tonic::transport::Channel;
use tracing::error;
use tracing::info;

use crate::sraft::api::grpc::RequestVoteRequest;

use super::api::grpc;
use super::api::grpc::sraft_client;
use super::api::grpc::sraft_client::SraftClient;

type PeerID = u32;

pub enum ServerState {
    Leader,
    Follower,
    Candidate,
}

pub enum Message {
    Get {
        key: String,
        resp: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    Set {
        key: String,
        value: Vec<u8>,
        resp: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    RequestVote {
        req: RequestVoteRequest,
        resp: oneshot::Sender<Result<Vote>>,
    },
    ReceiveVote(Vote),
}

struct LogEntry {
    entry: grpc::LogEntry,
    term: u64,
}

struct Vote {
    term: u64,
    granted: bool,
}

pub struct StateMachine {
    id: PeerID,
    rx_msgs: mpsc::Receiver<Message>,
    tx_msgs: mpsc::Sender<Message>,
    data: HashMap<String, Vec<u8>>,
    peers: HashMap<PeerID, sraft_client::SraftClient<Channel>>,
    election_timeout: Instant,

    // persistent state
    current_term: u64,
    voted_for: Option<PeerID>,
    log: Vec<LogEntry>,

    // volatile state
    state: ServerState,
    commit_idx: usize,
    last_applied_idx: usize,

    // volatile state on leader
    next_idx: Vec<(PeerID, usize)>,
    match_idx: Vec<(PeerID, usize)>,
}

impl StateMachine {
    pub fn new(
        id: PeerID,
        peers: HashMap<PeerID, String>,
        rx_msgs: mpsc::Receiver<Message>,
        tx_msgs: mpsc::Sender<Message>,
    ) -> Result<Self> {
        Ok(Self {
            id,
            rx_msgs,
            tx_msgs,
            data: HashMap::new(),
            peers: Self::connect_to_peers(peers)?,
            election_timeout: Self::next_election_timeout(),

            current_term: 0,
            voted_for: None,
            log: Vec::new(),

            state: ServerState::Follower,
            commit_idx: 0,
            last_applied_idx: 0,
            next_idx: Vec::new(),
            match_idx: Vec::new(),
        })
    }

    pub async fn run(&mut self) {
        loop {
            if self.maybe_apply_log() {
                continue;
            }
            match self.state {
                ServerState::Leader => self.run_leader().await,
                ServerState::Follower => self.run_follower().await,
                ServerState::Candidate => self.run_candidate().await,
            }
        }
    }

    async fn run_leader(&mut self) {
        todo!()
    }

    async fn run_follower(&mut self) {
        select! {
            _ = time::sleep_until(self.election_timeout) => {
                self.convert_to_candidate();
            },
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::Get { key, resp } => {
                        resp.send(self.get_data(&key));
                    }
                    Message::Set { key: _, value: _, resp } => {
                        let _ = resp.send(Err(anyhow!("i'm follower")));
                    }
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.vote(&req));
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as follower
                    }
                }
            }
        }
    }

    async fn run_candidate(&mut self) {
        todo!()
    }

    fn vote(&mut self, req: &RequestVoteRequest) -> Result<Vote> {
        if req.term < self.current_term {
            // candidate is stale
            return Ok(Vote {
                term: self.current_term,
                granted: false,
            });
        }
        if req.term > self.current_term {
            // candidate term is greater than ours, we are stale
            self.convert_to_follower(req.term);
            return Err(anyhow!("i'm stale"));
        }

        if self.voted_for.map_or(true, |v| v == req.candidate_id) {
            // if we haven't voted or or already voted for that candidate, vote for candidate
            // if it's log is at least up-to-date as ours
            return Ok(Vote {
                term: self.current_term,
                granted: self.log.last().map_or(0, |e| e.term) <= req.last_log_term,
            });
        }
        return Ok(Vote {
            term: self.current_term,
            granted: false,
        });
    }

    fn convert_to_candidate(&mut self) {
        info!(peer_id = self.id, "convering to candidate");
        self.state = ServerState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.election_timeout = Self::next_election_timeout();

        // request votes from all peers in parallel
        for (id, client) in self.peers.iter() {
            if self.id == *id {
                continue;
            }
            task::spawn(Self::request_vote(
                client.clone(),
                self.tx_msgs.clone(),
                self.current_term,
                self.id,
                self.log.len(),
                self.log.last().map_or(0, |e| e.term),
            ));
        }
    }

    fn convert_to_follower(&mut self, term: u64) {
        todo!();
    }

    async fn request_vote(
        mut client: SraftClient<Channel>,
        msgs: mpsc::Sender<Message>,
        term: u64,
        candidate_id: PeerID,
        last_log_index: usize,
        last_log_term: u64,
    ) {
        let req = RequestVoteRequest {
            term,
            candidate_id,
            last_log_index: last_log_index as u64,
            last_log_term,
        };
        match client.request_vote(req).await {
            Ok(repl) => {
                let repl = repl.into_inner();
                let msg = Message::ReceiveVote(Vote {
                    term: repl.term,
                    granted: repl.vote_granted,
                });
                let _ = msgs.send(msg).await;
            }
            Err(err) => {
                error!(
                    candidate = format!("{candidate_id}"),
                    error = format!("{err:#}"),
                    "requesting vote",
                );
            }
        }
    }

    fn get_data(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned()) // TODO: storage can be shared with grpc layer probably?
    }

    fn maybe_apply_log(&mut self) -> bool {
        if self.commit_idx > self.last_applied_idx {
            self.last_applied_idx += 1;
            debug_assert!(
                self.last_applied_idx < self.log.len(),
                "last_applied_idx >= log length"
            );
            let entry = self.log[self.last_applied_idx].entry.clone();
            self.data.insert(entry.key, entry.value);
            return true;
        }
        return false;
    }

    fn connect_to_peers(
        addrs: HashMap<PeerID, String>,
    ) -> Result<HashMap<PeerID, SraftClient<Channel>>> {
        let mut peers = HashMap::with_capacity(addrs.len());
        for addr in addrs {
            let ch = Channel::from_shared(addr.1)?.connect_lazy();
            peers.insert(addr.0, SraftClient::new(ch));
        }
        Ok(peers)
    }

    fn next_election_timeout() -> Instant {
        Instant::now()
            .checked_add(Duration::from_millis(rng().random_range(200..300)))
            .unwrap()
    }
}
