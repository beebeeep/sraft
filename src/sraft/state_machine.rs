use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

use super::api::grpc::{self, sraft_client::SraftClient};
use anyhow::{anyhow, Result};
use rand::rng;
use rand::Rng;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;
use tokio::time::Instant;
use tonic::transport::Channel;
use tracing::{error, info};

type PeerID = u32;

pub enum ServerState {
    Leader,
    Follower,
    Candidate,
}

pub enum Message {
    // messages from grpc
    Get {
        key: String,
        resp: oneshot::Sender<Result<Option<Vec<u8>>>>, // TODO: use pb types?
    },
    Set {
        key: String,
        value: Vec<u8>,
        resp: oneshot::Sender<Result<Option<Vec<u8>>>>, // TODO: use pb types?
    },
    RequestVote {
        req: grpc::RequestVoteRequest,
        resp: oneshot::Sender<Result<grpc::RequestVoteResponse>>,
    },
    AppendEntries {
        req: grpc::AppendEntriesRequest,
        resp: oneshot::Sender<Result<grpc::AppendEntriesResponse>>,
    },

    // messages from internal async jobs
    ReceiveVote(grpc::RequestVoteResponse),
}

struct LogEntry {
    entry: grpc::LogEntry,
    term: u64,
}

pub struct StateMachine {
    id: PeerID,
    rx_msgs: mpsc::Receiver<Message>,
    tx_msgs: mpsc::Sender<Message>,
    data: HashMap<String, Vec<u8>>,
    peers: HashMap<PeerID, SraftClient<Channel>>,
    quorum: u32,
    election_timeout: Instant,

    // persistent state
    current_term: u64,
    voted_for: Option<PeerID>,
    log: Vec<LogEntry>,

    // volatile state
    state: ServerState,
    commit_idx: usize,
    last_applied_idx: usize,

    // volatile state on candidate
    votes_received: u32,

    // volatile state on leader
    next_idx: HashMap<PeerID, usize>,
    match_idx: HashMap<PeerID, usize>,
}

impl Display for StateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Peer(id={}, term={})", self.id, self.current_term)
    }
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
            quorum: (peers.len() / 2 + 1) as u32,
            peers: Self::connect_to_peers(peers)?,
            election_timeout: Self::next_election_timeout(),

            current_term: 0,
            voted_for: None,
            log: Vec::new(),

            votes_received: 0,

            state: ServerState::Follower,
            commit_idx: 0,
            last_applied_idx: 0,
            next_idx: HashMap::new(),
            match_idx: HashMap::new(),
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
                        let _ = resp.send(self.get_data(&key));
                    }
                    Message::Set { key: _, value: _, resp } => {
                        let _ = resp.send(Err(anyhow!("i'm follower")));
                    }
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.vote(&req));
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as follower
                    },
                    Message::AppendEntries{req, resp } => {
                        let _ = resp.send(self.append_entries(&req));
                    },
                }
            }
        }
    }

    async fn run_candidate(&mut self) {
        select! {
            _ = time::sleep_until(self.election_timeout) => {
                self.convert_to_candidate();
            },
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::Get { key: _, resp } => {
                        let _ = resp.send(Err(anyhow!("election time")));
                    }
                    Message::Set { key: _, value: _, resp } => {
                        let _ = resp.send(Err(anyhow!("election time")));
                    }
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.vote(&req));
                    },
                    Message::AppendEntries{req, resp } => {
                        if req.term >= self.current_term {
                            // leader was elected and already send AppendEntries
                            self.set_term(req.term);
                            self.convert_to_follower();
                        }
                        let _ = resp.send(self.append_entries(&req));
                    },
                    Message::ReceiveVote(vote) => {
                        if vote.term > self.current_term {
                            // we are stale
                            self.set_term(vote.term);
                            self.convert_to_follower()
                        } else if vote.term == self.current_term && vote.vote_granted {
                            info!(votes_received = self.votes_received, "got vote");
                            self.votes_received += 1;
                            if self.votes_received >= self.quorum {
                                self.convert_to_leader();
                            }
                        }
                    },
                }
            }
        }
    }

    fn append_entries(
        &mut self,
        req: &grpc::AppendEntriesRequest,
    ) -> Result<grpc::AppendEntriesResponse> {
        if req.term < self.current_term {
            // stale leader, reject RPC
            return Ok(grpc::AppendEntriesResponse {
                term: self.current_term,
                success: false,
            });
        }
        todo!()
    }

    fn set_term(&mut self, term: u64) {
        self.current_term = term;
        self.voted_for = None;
    }

    fn vote(&mut self, req: &grpc::RequestVoteRequest) -> Result<grpc::RequestVoteResponse> {
        if req.term < self.current_term {
            // candidate is stale
            return Ok(grpc::RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            });
        }
        if req.term > self.current_term {
            // candidate is more recent
            self.set_term(req.term);
            self.convert_to_follower();
        }

        if self.voted_for.map_or(true, |v| v == req.candidate_id)
            && self.log.last().map_or(0, |e| e.term) <= req.last_log_term
        {
            // if we haven't voted or or already voted for that candidate, vote for candidate
            // if it's log is at least up-to-date as ours
            self.voted_for = Some(req.candidate_id);
            return Ok(grpc::RequestVoteResponse {
                term: self.current_term,
                vote_granted: true,
            });
        }
        Ok(grpc::RequestVoteResponse {
            term: self.current_term,
            vote_granted: false,
        })
    }

    fn convert_to_candidate(&mut self) {
        info!(peer = %self, "converting to candidate");
        self.state = ServerState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received = 1;
        self.election_timeout = Self::next_election_timeout();

        // request votes from all peers in parallel
        for (id, client) in self.peers.iter() {
            if self.id == *id {
                continue;
            }
            task::spawn(Self::request_vote(
                client.clone(),
                *id,
                self.tx_msgs.clone(),
                self.current_term,
                self.id,
                self.last_log_index(),
                self.log.last().map_or(0, |e| e.term),
            ));
        }
    }

    fn convert_to_follower(&mut self) {
        info!(peer = %self, "converting to follower");
        self.state = ServerState::Follower;
    }

    fn convert_to_leader(&mut self) {
        info!(peer = %self, "converting to leader");
        self.state = ServerState::Leader;
        self.next_idx.clear();
        self.match_idx.clear();
        for (peer, _) in self.peers.iter() {
            self.next_idx.insert(*peer, self.last_log_index() + 1);
            self.match_idx.insert(*peer, 0);
        }
    }

    fn last_log_index(&self) -> usize {
        self.log.len() //   just to remind that we count indexes from 1
    }

    async fn request_vote(
        mut client: SraftClient<Channel>,
        peer_id: PeerID,
        msgs: mpsc::Sender<Message>,
        term: u64,
        candidate_id: PeerID,
        last_log_index: usize,
        last_log_term: u64,
    ) {
        let req = grpc::RequestVoteRequest {
            term,
            candidate_id,
            last_log_index: last_log_index as u64,
            last_log_term,
        };
        match client.request_vote(req).await {
            Ok(repl) => {
                let repl = repl.into_inner();
                let msg = Message::ReceiveVote(grpc::RequestVoteResponse {
                    term: repl.term,
                    vote_granted: repl.vote_granted,
                });
                let _ = msgs.send(msg).await;
            }
            Err(err) => {
                error!(
                    candidate = candidate_id,
                    peer_id = peer_id,
                    error = format!("{err:#}"),
                    "requesting vote from peer",
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
        false
    }

    fn connect_to_peers(
        addrs: HashMap<PeerID, String>,
    ) -> Result<HashMap<PeerID, SraftClient<Channel>>> {
        let mut peers = HashMap::with_capacity(addrs.len());
        for addr in addrs {
            println!("connected to {}", addr.1);
            let ch = Channel::from_shared(addr.1)?.connect_lazy();
            peers.insert(addr.0, SraftClient::new(ch));
        }
        Ok(peers)
    }

    fn next_election_timeout() -> Instant {
        Instant::now()
            .checked_add(Duration::from_millis(rng().random_range(1000..1200)))
            .unwrap()
    }
}
