use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Range;
use std::time::Duration;

use super::api::grpc::{self, sraft_client::SraftClient};
use anyhow::{anyhow, Result};
use rand::distr::uniform::SampleRange;
use rand::rng;
use rand::Rng;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;
use tokio::time::Instant;
use tonic::transport::Channel;
use tracing::debug;
use tracing::warn;
use tracing::{error, info};

const IDLE_TIMEOUT: Duration = Duration::from_millis(2000);
const ELECTION_TIMEOUT_MS: Range<u64> = 2500..3000;

type PeerID = u32;
type EntryIdx = usize; // log entry index, STARTS FROM 1

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
    AppendEntriesResponse {
        peer_id: PeerID,
        entries_count: usize,
        response: grpc::AppendEntriesResponse,
    },
}

pub struct StateMachine {
    id: PeerID,
    rx_msgs: mpsc::Receiver<Message>,
    tx_msgs: mpsc::Sender<Message>,
    data: HashMap<String, Vec<u8>>,
    peers: HashMap<PeerID, SraftClient<Channel>>,
    quorum: u32,
    election_timeout: Instant,
    pending_transactions: HashMap<EntryIdx, oneshot::Sender<Option<Vec<u8>>>>,

    // persistent state
    current_term: u64,
    voted_for: Option<PeerID>,
    log: Vec<grpc::LogEntry>,

    // volatile state
    state: ServerState,
    commit_idx: EntryIdx,
    last_applied_idx: EntryIdx,

    // volatile state on candidate
    votes_received: u32,

    // volatile state on leader
    next_idx: HashMap<PeerID, EntryIdx>,
    match_idx: HashMap<PeerID, EntryIdx>,
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
            election_timeout: Self::next_election_timeout(Some(100..200)),
            pending_transactions: HashMap::new(),

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
        self.maybe_update_followers();
        self.maybe_update_commit_idx();

        select! {
            _ = time::sleep(IDLE_TIMEOUT) => {
                self.send_heartbeat();
            },
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::Get { key, resp } => {
                        let _ = resp.send(self.get_data(&key));
                    }
                    Message::Set { key, value, resp } => {
                        self.set_data(&key, value, resp);
                    }
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.request_vote(req)); // NB: not clear what shall we do here?
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as we are leader already
                    },
                    Message::AppendEntries{req, resp } => {
                        if req.term > self.current_term {
                            info!(peer = %self, new_leader_id = req.leader_id, new_leader_term = req.term, "found new leader with greated term, converting to follower");
                            self.convert_to_follower();
                            let _ = resp.send(self.append_entries(req));
                        } else {
                            info!(peer = %self, offender_id = req.leader_id, offender_term = req.term, "found unexpected leader");
                        }
                    },
                    Message::AppendEntriesResponse{peer_id, entries_count, response} => {
                        incorrect!!!!!!!!!
                        info!(
                            peer = %self,
                            follower = peer_id,
                            success = response.success,
                            "AppendEntries response"
                        );
                        if response.success {
                            let next_idx  = self.next_idx.get_mut(&peer_id).unwrap();
                            *self.match_idx.get_mut(&peer_id).unwrap() = *next_idx;
                            *next_idx += entries_count;
                        } else {
                            // follower failed AppendEntries, will retry with earlier log entries
                            *self.next_idx.get_mut(&peer_id).unwrap() -= 1;
                        }
                    }
                }
            }
        }
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
                        let _ = resp.send(Err(anyhow!("i'm follower")));    // TODO: proxy request to leader?
                    }
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.request_vote(req));
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as follower
                    },
                    Message::AppendEntries{req, resp } => {
                        if req.term > self.current_term {
                            self.set_term(req.term);
                        }
                        let _ = resp.send(self.append_entries(req));
                    },
                    Message::AppendEntriesResponse{response: _, entries_count: _, peer_id: _} => {
                        // don't care as follower
                    }
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
                        let _ = resp.send(self.request_vote(req));
                    },
                    Message::AppendEntries{req, resp } => {
                        if req.term > self.current_term {
                            // leader was elected and already send AppendEntries
                            self.set_term(req.term);
                            self.convert_to_follower();
                        }
                        let _ = resp.send(self.append_entries(req));
                    },
                    Message::AppendEntriesResponse{response: _, entries_count: _, peer_id: _} => {
                        // don't care as candidate
                    }
                    Message::ReceiveVote(vote) => {
                        info!(peer = %self, votes_received = self.votes_received, vote_granted = vote.vote_granted, "vote result");
                        if vote.term > self.current_term {
                            // we are stale
                            self.set_term(vote.term);
                            self.convert_to_follower()
                        } else if vote.term == self.current_term && vote.vote_granted {
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
        req: grpc::AppendEntriesRequest,
    ) -> Result<grpc::AppendEntriesResponse> {
        if !req.entries.is_empty() {
            debug!(peer = %self, req_term = req.term, prev_log_index = req.prev_log_index, prev_log_term = req.prev_log_index, "got appendEntries");
        }
        if req.term < self.current_term {
            // stale leader, reject RPC
            return Ok(grpc::AppendEntriesResponse {
                term: self.current_term,
                success: false,
            });
        }
        self.election_timeout = Self::next_election_timeout(None);

        let prev_log_index = req.prev_log_index as EntryIdx;

        if prev_log_index == 0 {
            // special case: we just bootstraped the cluster and log is empty
            debug_assert!(self.log.is_empty());
            self.log.extend(req.entries);
            if req.leader_commit as EntryIdx > self.commit_idx {
                self.commit_idx = usize::min(req.leader_commit as usize, self.last_log_index());
            }
            return Ok(grpc::AppendEntriesResponse {
                term: self.current_term,
                success: true,
            });
        }

        if prev_log_index > self.log.len() || self.log[prev_log_index - 1].term != req.prev_log_term
        {
            // we don't have matching log entry at prev_log_index
            return Ok(grpc::AppendEntriesResponse {
                term: self.current_term,
                success: false,
            });
        }

        for (i, entry) in req.entries.into_iter().enumerate() {
            if let Some(e) = self.log.get(prev_log_index + i) {
                if e.term == entry.term {
                    // this entry match, skipping
                    continue;
                } else {
                    // conflicting entry, truncate it and all that follow
                    self.log.truncate(prev_log_index + i);
                }
            }
            self.log.push(entry);
        }

        if req.leader_commit as EntryIdx > self.commit_idx {
            self.commit_idx = usize::min(req.leader_commit as usize, self.last_log_index());
        }

        Ok(grpc::AppendEntriesResponse {
            term: self.current_term,
            success: true,
        })
    }

    fn request_vote(&mut self, req: grpc::RequestVoteRequest) -> Result<grpc::RequestVoteResponse> {
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
            && self.last_log_term() <= req.last_log_term
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

    fn set_term(&mut self, term: u64) {
        self.current_term = term;
        self.voted_for = None;
    }

    fn convert_to_candidate(&mut self) {
        info!(peer = %self, "converting to candidate");
        self.state = ServerState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received = 1;
        self.election_timeout = Self::next_election_timeout(None);
        self.next_idx.clear();
        self.match_idx.clear();

        // request votes from all peers in parallel
        for (id, client) in self.peers.iter() {
            if self.id == *id {
                continue;
            }
            task::spawn(Self::request_vote_from_peer(
                client.clone(),
                *id,
                self.tx_msgs.clone(),
                self.current_term,
                self.id,
                self.last_log_index(),
                self.last_log_term(),
            ));
        }
    }

    fn convert_to_follower(&mut self) {
        info!(peer = %self, "converting to follower");
        self.state = ServerState::Follower;
        self.next_idx.clear();
        self.match_idx.clear();
        self.pending_transactions.clear();
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
        self.send_heartbeat();
    }

    fn last_log_index(&self) -> EntryIdx {
        self.log.len() //   just to remind that we count indexes from 1
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    async fn request_vote_from_peer(
        mut client: SraftClient<Channel>,
        peer_id: PeerID,
        msgs: mpsc::Sender<Message>,
        term: u64,
        candidate_id: PeerID,
        last_log_index: EntryIdx,
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
                    error = %err,
                    "requesting vote from peer",
                );
            }
        }
    }

    fn send_heartbeat(&self) {
        for peer in self.peers.keys() {
            if self.id == *peer {
                continue;
            }
            self.send_entries(peer, &[]);
        }
    }

    fn send_entries(&self, peer: &PeerID, entries: &[grpc::LogEntry]) {
        let client = self.peers.get(peer).unwrap();
        let msgs = self.tx_msgs.clone();
        let mut client = client.clone();
        let _self = format!("{self}");
        let follower = *peer;
        let prev_log_idx = self.next_idx.get(&follower).unwrap() - 1;
        let entries_count = entries.len();
        let req = grpc::AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index: prev_log_idx as u64,
            prev_log_term: self.log.get(prev_log_idx).map_or(0, |e| e.term),
            entries: entries.to_vec(),
            leader_commit: self.commit_idx as u64,
        };
        task::spawn(async move {
            match client.append_entries(req).await {
                Ok(resp) => {
                    let _ = msgs
                        .send(Message::AppendEntriesResponse {
                            peer_id: follower,
                            entries_count,
                            response: resp.into_inner(),
                        })
                        .await;
                }
                Err(err) => {
                    // retries are supposed to be part of raft logic itself
                    error!(
                        peer = _self,
                        follower = follower,
                        error = %err,
                        "sending AppendEntries"
                    );
                }
            }
        });
    }

    fn get_data(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned()) // TODO: storage can be shared with grpc layer probably?
    }

    fn set_data(
        &mut self,
        key: &str,
        value: Vec<u8>,
        resp: oneshot::Sender<Result<Option<Vec<u8>>>>,
    ) {
        self.log.push(grpc::LogEntry {
            term: self.current_term,
            command: Some(grpc::Command {
                key: key.to_string(),
                value,
            }),
        });

        // the transaction was inserted to log and will be sent to all followers in next iteration of leader loop
        // following future will wait for it to be commited and will respond to client
        let (tx, mut rx) = oneshot::channel();
        self.pending_transactions.insert(self.last_log_index(), tx);
        tokio::spawn(async move {
            select! {
                // TODO: process client disconnects via cancellation tokens,
                // otherwise we will be leaking memory in self.pending_transactions
                tx_result = &mut rx => {
                    warn!("tx_result");
                    match tx_result {
                        Ok(prev_value) => {
                            let _ = resp.send(Ok(prev_value));
                        }
                        Err(err) => {
                            error!(err = %err, "Set() failed");
                            let _ = resp.send(Err(err.into()));
                        }
                    }

                },
            }
            warn!("reporter done");
        });
    }

    fn maybe_apply_log(&mut self) -> bool {
        if self.commit_idx <= self.last_applied_idx {
            return false;
        }
        self.last_applied_idx += 1;
        debug_assert!(
            self.last_applied_idx <= self.log.len(),
            "(applying log entries) last_applied_idx > log length"
        );

        debug!(peer = %self, index = self.last_applied_idx, "applying log entry");

        let cmd = self.log[self.last_applied_idx - 1].command.clone().unwrap();
        let old_value = self.data.insert(cmd.key, cmd.value);
        if let Some(chan) = self.pending_transactions.remove(&self.last_applied_idx) {
            let _ = chan.send(old_value);
        }
        true
    }

    fn maybe_update_followers(&self) {
        let last_log_idx = self.last_log_index();
        for (follower, idx) in self.next_idx.iter() {
            debug!(peer = %self, follower = follower, follower_idx = idx, "follower nextIndex");
            if *idx < last_log_idx {
                debug!(peer = %self, follower = follower, follower_next_idx = idx, "updating follower");
                self.send_entries(follower, &self.log[*idx..]);
            }
        }
    }

    fn maybe_update_commit_idx(&mut self) {
        debug_assert!(
            self.commit_idx <= self.last_log_index(),
            "(updating commitIndex) commitIndex <= log len"
        );
        if self.commit_idx == self.last_log_index() {
            return;
        }
        let mut i = self.last_log_index();
        while i > self.commit_idx {
            let in_sync = self.match_idx.iter().filter(|(_, m)| **m >= i).count();
            if in_sync >= self.quorum as usize && self.log[i - 1].term == self.current_term {
                debug!(peer = %self, index = i, in_sync = in_sync, "got quorum on log entry");
                self.commit_idx = i;
                break;
            }
            i -= 1;
        }
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

    fn next_election_timeout(r: Option<Range<u64>>) -> Instant {
        Instant::now()
            .checked_add(Duration::from_millis(
                rng().random_range(r.unwrap_or(ELECTION_TIMEOUT_MS)),
            ))
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_entries() {
        let (tx, rx) = mpsc::channel(1);
        let mut sm = StateMachine::new(0, HashMap::new(), rx, tx).unwrap();
        sm.set_term(2);
        let mut req = grpc::AppendEntriesRequest {
            term: 1,
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        // stale term
        let mut resp = sm.append_entries(req.clone()).unwrap();
        assert!(!resp.success);

        // ok term, no entries
        req.term = 2;
        resp = sm.append_entries(req.clone()).unwrap();
        assert!(resp.success);
        assert!(sm.log.is_empty());

        // insert entry
        req.leader_commit = 1;
        req.entries.push(grpc::LogEntry {
            term: 2,
            command: Some(grpc::Command {
                key: "foo".to_string(),
                value: "chlos".into(),
            }),
        });
        resp = sm.append_entries(req.clone()).unwrap();
        assert!(resp.success);
        assert_eq!(sm.log.len(), 1);
        assert_eq!(sm.log[0].command.as_ref().unwrap().key, "foo");
        assert_eq!(sm.commit_idx, 1);

        // another entry
        req.leader_commit = 2;
        req.prev_log_index = 1;
        req.prev_log_term = 2;
        resp = sm.append_entries(req.clone()).unwrap();
        assert!(resp.success);
        assert_eq!(sm.log.len(), 2);
        assert_eq!(sm.commit_idx, 2);

        // TODO: overwrite conflicting entries
        sm.log.push(grpc::LogEntry {
            term: 42,
            command: Some(grpc::Command {
                key: "wrong".to_string(),
                value: "wrong".into(),
            }),
        });
        assert_eq!(sm.log[2].command.as_ref().unwrap().key, "wrong");
        sm.commit_idx = 3;
        req.leader_commit = 3;
        req.prev_log_index = 2;
        resp = sm.append_entries(req.clone()).unwrap();
        assert!(resp.success);
        assert_eq!(sm.log.len(), 3);
        assert_eq!(sm.commit_idx, 3);
        assert_eq!(sm.log[2].command.as_ref().unwrap().key, "foo");
    }
}
