use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Range;
use std::time::Duration;

use super::api::grpc::{self, sraft_client::SraftClient};
use crate::sraft::storage;
use anyhow::{anyhow, Context, Result};
use rand::rng;
use rand::Rng;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;
use tokio::time::Instant;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

const IDLE_TIMEOUT: Duration = Duration::from_millis(1500); // time between idling follower heartbeats
const TICK_TIMEOUT: Duration = Duration::from_millis(300); // main leader loop timeout
const ELECTION_TIMEOUT_MS: Range<u64> = 2500..3000;
const UPDATE_TIMEOUT: Duration = Duration::from_millis(300);

type PeerID = usize;
type EntryIdx = usize; // log entry index, STARTS FROM 1

struct PeerState {
    client: SraftClient<Channel>,
    next_idx: EntryIdx,
    match_idx: EntryIdx,
    update_timeout: Option<Instant>,
    next_heartbeat: Instant,
}

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
        replicated_index: Option<EntryIdx>,
    },
}

pub struct StateMachine<S: storage::Store> {
    id: PeerID,
    rx_msgs: mpsc::Receiver<Message>,
    tx_msgs: mpsc::Sender<Message>,
    // data: HashMap<String, Vec<u8>>,
    peers: Vec<PeerState>,
    quorum: u32,
    election_timeout: Instant,
    pending_transactions: HashMap<EntryIdx, oneshot::Sender<Option<Vec<u8>>>>,

    // persistent state
    // current_term: u64,
    // voted_for: Option<PeerID>,
    // log: Vec<grpc::LogEntry>,
    storage: S,

    // volatile state
    state: ServerState,
    commit_idx: EntryIdx,
    last_applied_idx: EntryIdx,

    // volatile state on candidate
    votes_received: u32,
}

impl<S: storage::Store> Display for StateMachine<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(i={}, t={}, ll={}, ci={}, la={})",
            match self.state {
                ServerState::Leader => "L",
                _ => "F",
            },
            self.id,
            self.storage.current_term(),
            self.storage.last_log_idx(),
            self.commit_idx,
            self.last_applied_idx
        )
    }
}

impl<S: storage::Store + 'static> StateMachine<S> {
    pub fn new(
        id: PeerID,
        peers: Vec<String>,
        store: S,
        rx_msgs: mpsc::Receiver<Message>,
        tx_msgs: mpsc::Sender<Message>,
    ) -> Result<Self> {
        Ok(Self {
            id,
            rx_msgs,
            tx_msgs,
            quorum: (peers.len() / 2 + 1) as u32,
            peers: Self::init_peers(peers)?,
            election_timeout: Self::next_election_timeout(Some(100..200)),
            pending_transactions: HashMap::new(),
            storage: store,
            votes_received: 0,
            state: ServerState::Follower,
            commit_idx: 0,
            last_applied_idx: 0,
        })
    }

    pub async fn run(&mut self) {
        loop {
            match self.maybe_apply_log().await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(err) => {
                    error!(error = format!("{err:#}"), "applying log");
                }
            }
            let err = match self.state {
                ServerState::Leader => self.run_leader().await,
                ServerState::Follower => self.run_follower().await,
                ServerState::Candidate => self.run_candidate().await,
            };
            match err {
                Ok(_) => {}
                Err(err) => error!(error = format!("{err:#}"), "got error"),
            }
        }
    }

    async fn run_leader(&mut self) -> Result<()> {
        self.maybe_update_followers()
            .await
            .context("updating followers")?;
        self.maybe_update_commit_idx()
            .await
            .context("updating commitIndex")?;

        select! {
            _ = time::sleep(TICK_TIMEOUT) => {},
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::Get { key, resp } => {
                        self.get_data(&key, resp).await?;
                    }
                    Message::Set { key, value, resp } => {
                         self.set_data(&key, value, resp).await?;
                    }
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.request_vote(req).await.context("voting for candidate")); // NB: not clear what shall we do here?
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as we are leader already
                    },
                    Message::AppendEntries{req, resp } => {
                        if req.term > self.storage.current_term() {
                            info!(new_leader_id = req.leader_id, new_leader_term = req.term, "found new leader with greated term, converting to follower");
                            self.convert_to_follower();
                            let _ = resp.send(self.append_entries(req).await.context("appending entries"));
                        } else {
                            info!(offender_id = req.leader_id, offender_term = req.term, "found unexpected leader");
                        }
                    },
                    Message::AppendEntriesResponse{peer_id, replicated_index} => {
                        let Some(peer) = self.peers.get_mut(peer_id) else {
                            return Err(anyhow!("got AppendEntries response from unknown peer {peer_id}"));
                        };
                        if let Some(replicated_index) = replicated_index {
                            debug!(
                                follower = peer_id,
                                replicated_index = replicated_index,
                                "follower replicated entries"
                            );
                                peer.next_idx = replicated_index + 1;
                                peer.match_idx = replicated_index;
                                peer.update_timeout = None;
                        } else {
                            // follower failed AppendEntries, will retry with earlier log entries
                            debug!(
                                follower = peer_id,
                                "follower is lagging"
                            );
                            peer.next_idx -= 1;
                            peer.update_timeout = None;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_follower(&mut self) -> Result<()> {
        select! {
            _ = time::sleep_until(self.election_timeout) => {
                self.convert_to_candidate().await.context("converting to candidate")?;
            },
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::Get { key, resp } => {
                        // NB: reads from followers are eventually consistent and may lag behind leader
                        // even in normal mode (right after leader got quorum and committed write)
                        let _ = resp.send(self.storage.get(&key).await);
                    }
                    Message::Set { key: _, value: _, resp } => {
                        let _ = resp.send(Err(anyhow!("i'm follower")));    // TODO: proxy request to leader?
                    }
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.request_vote(req).await.context("requesting vote"));
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as follower
                    },
                    Message::AppendEntries{req, resp } => {
                        if req.term > self.storage.current_term() {
                            self.set_term(req.term).await.context("setting term")?;
                        }
                        let _ = resp.send(self.append_entries(req).await.context("processing AppendEntries"));
                    },
                    Message::AppendEntriesResponse{ .. } => {
                        // don't care as follower
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_candidate(&mut self) -> Result<()> {
        select! {
            _ = time::sleep_until(self.election_timeout) => {
                self.convert_to_candidate().await.context("converting to candidate")?;
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
                        let _ = resp.send(self.request_vote(req).await.context("requesting vote"));
                    },
                    Message::AppendEntries{req, resp } => {
                        if req.term > self.storage.current_term() {
                            // leader was elected and already send AppendEntries
                            self.set_term(req.term).await.context("setting new term")?;
                            debug!(term = req.term, "got AppendEntries with greater term");
                            self.convert_to_follower();
                        }
                        let _ = resp.send(self.append_entries(req).await.context("processing AppendEntries"));
                    },
                    Message::AppendEntriesResponse { .. } => {
                        // don't care as candidate
                    }
                    Message::ReceiveVote(vote) => {
                        info!(votes_received = self.votes_received, vote_granted = vote.vote_granted, "vote result");
                        if vote.term > self.storage.current_term() {
                            // we are stale
                            self.set_term(vote.term).await.context("setting new term")?;
                            debug!(term = vote.term, "got RequestVote reply with greater term");
                            self.convert_to_follower()
                        } else if vote.term == self.storage.current_term() && vote.vote_granted {
                            self.votes_received += 1;
                            if self.votes_received >= self.quorum {
                                self.convert_to_leader();
                            }
                        }
                    },
                }
            }
        }
        Ok(())
    }

    async fn append_entries(
        &mut self,
        req: grpc::AppendEntriesRequest,
    ) -> Result<grpc::AppendEntriesResponse> {
        if req.term < self.storage.current_term() {
            // stale leader, reject RPC
            return Ok(grpc::AppendEntriesResponse {
                term: self.storage.current_term(),
                success: false,
            });
        }
        debug!(
            leader_commit = req.leader_commit,
            entries_count = req.entries.len(),
            prev_log_idx = req.prev_log_index,
            "AppendEntries"
        );
        self.election_timeout = Self::next_election_timeout(None);

        let prev_log_index = req.prev_log_index as EntryIdx;

        if prev_log_index != 0
            && (prev_log_index > self.storage.last_log_idx()
                || self.storage.get_log_entry(prev_log_index).await?.term != req.prev_log_term)
        {
            debug!(state = %self, prev_log_index = prev_log_index, prev_log_term = req.prev_log_term, "i'm lagging?");
            // we don't have matching log entry at prev_log_index
            return Ok(grpc::AppendEntriesResponse {
                term: self.storage.current_term(),
                success: false,
            });
        }

        for (i, entry) in req.entries.into_iter().enumerate() {
            let next_entry_idx = prev_log_index + 1 + i;
            if self.storage.last_log_idx() < next_entry_idx {
                self.storage
                    .append_to_log(entry)
                    .await
                    .context("appending log entry")?;
                continue;
            }

            let existing_entry = self
                .storage
                .get_log_entry(next_entry_idx)
                .await
                .context("getting existing log entry")?;
            if existing_entry.term != entry.term {
                // conflicting entry, truncate it and all that follow
                self.storage
                    .truncate_log(next_entry_idx)
                    .await
                    .context("removing conflicting entries from log")?;
            }
        }

        if req.leader_commit as EntryIdx > self.commit_idx {
            self.commit_idx = usize::min(req.leader_commit as usize, self.storage.last_log_idx());
        }

        Ok(grpc::AppendEntriesResponse {
            term: self.storage.current_term(),
            success: true,
        })
    }

    async fn request_vote(
        &mut self,
        req: grpc::RequestVoteRequest,
    ) -> Result<grpc::RequestVoteResponse> {
        if req.term < self.storage.current_term() {
            // candidate is stale
            return Ok(grpc::RequestVoteResponse {
                term: self.storage.current_term(),
                vote_granted: false,
            });
        }
        if req.term > self.storage.current_term() {
            // candidate is more recent
            self.set_term(req.term).await.context("setting new term")?;
            debug!(term = req.term, "got RequestVote with greater term");
            self.convert_to_follower();
        }

        if self
            .storage
            .voted_for()
            .map_or(true, |v| v == req.candidate_id as usize)
            && self.storage.last_log_term() <= req.last_log_term
        {
            // if we haven't voted or or already voted for that candidate, vote for candidate
            // if it's log is at least up-to-date as ours
            self.storage
                .set_voted_for(req.candidate_id as usize)
                .await?;
            return Ok(grpc::RequestVoteResponse {
                term: self.storage.current_term(),
                vote_granted: true,
            });
        }
        Ok(grpc::RequestVoteResponse {
            term: self.storage.current_term(),
            vote_granted: false,
        })
    }

    async fn set_term(&mut self, term: u64) -> Result<()> {
        self.storage.set_current_term(term).await?;
        self.storage.reset_voted_for().await
    }

    async fn convert_to_candidate(&mut self) -> Result<()> {
        info!("converting to candidate");
        self.state = ServerState::Candidate;
        self.storage
            .set_current_term(self.storage.current_term() + 1)
            .await
            .context("incrementing term")?;
        self.storage
            .set_voted_for(self.id)
            .await
            .context("voting for myself")?;
        self.votes_received = 1;
        self.election_timeout = Self::next_election_timeout(None);

        // request votes from all peers in parallel
        for (id, peer) in self.peers.iter().enumerate() {
            if self.id == id {
                continue;
            }
            task::spawn(Self::request_vote_from_peer(
                peer.client.clone(),
                id,
                self.tx_msgs.clone(),
                self.storage.current_term(),
                self.id,
                self.storage.last_log_idx(),
                self.storage.last_log_term(),
            ));
        }
        Ok(())
    }

    fn convert_to_follower(&mut self) {
        info!("converting to follower");
        self.state = ServerState::Follower;
        self.pending_transactions.clear();
    }

    fn convert_to_leader(&mut self) {
        info!("converting to leader");
        self.state = ServerState::Leader;
        for peer in &mut self.peers {
            peer.next_idx = self.storage.last_log_idx() + 1;
            peer.update_timeout = None;
            peer.next_heartbeat = Instant::now();
            peer.match_idx = 0;
        }
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
            candidate_id: candidate_id as u32,
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

    async fn send_entries(&mut self, peer: PeerID, entries: Vec<grpc::LogEntry>) -> Result<()> {
        let mut client = self.peers[peer].client.clone();
        let msgs = self.tx_msgs.clone();
        let _self = format!("{self}");
        let prev_log_idx = self.peers[peer].next_idx - 1;

        let entries_count = entries.len();
        let update_timeout = Instant::now() + UPDATE_TIMEOUT;
        self.peers[peer].update_timeout = Some(update_timeout);
        self.peers[peer].next_heartbeat = Instant::now() + IDLE_TIMEOUT;
        let prev_log_term = if prev_log_idx == 0 {
            0
        } else {
            self.storage
                .get_log_entry(prev_log_idx)
                .await
                .context("getting previous log entry")?
                .term
        };
        let req = grpc::AppendEntriesRequest {
            term: self.storage.current_term(),
            leader_id: self.id as u32,
            prev_log_index: prev_log_idx as u64,
            prev_log_term,
            entries,
            leader_commit: self.commit_idx as u64,
        };
        task::spawn(async move {
            select! {
                _ = time::sleep_until(update_timeout) => {
                    warn!(follower = peer, "AppendEntries timed out")
                },
                resp = client.append_entries(req) => {
                    match resp {
                        Ok(resp) => {
                            let _ = msgs
                                .send(Message::AppendEntriesResponse {
                                    peer_id: peer,
                                    replicated_index: if resp.into_inner().success {
                                        Some(prev_log_idx + entries_count)
                                    } else {
                                        None
                                    },
                                })
                                .await;
                        }
                        Err(err) => {
                            // retries are supposed to be part of raft logic itself
                            error!(follower = peer, error = format!("{err:#}"), "sending AppendEntries" );
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn get_data(
        &mut self,
        key: &str,
        resp: oneshot::Sender<Result<Option<Vec<u8>>>>,
    ) -> Result<()> {
        match self
            .storage
            .get(key)
            .await
            .context("getting data from storage")
        {
            Ok(data) => {
                let _ = resp.send(Ok(data));
                Ok(())
            }
            Err(err) => {
                let _ = resp.send(Err(anyhow!("{err:#}")));
                Err(err)
            }
        }
    }
    async fn set_data(
        &mut self,
        key: &str,
        value: Vec<u8>,
        resp: oneshot::Sender<Result<Option<Vec<u8>>>>,
    ) -> Result<()> {
        if let Err(err) = self
            .storage
            .append_to_log(grpc::LogEntry {
                term: self.storage.current_term(),
                command: Some(grpc::Command {
                    key: key.to_string(),
                    value,
                }),
            })
            .await
        {
            let _ = resp.send(Err(anyhow!("saving entry to leader's log: {err:#}")));
            return Err(anyhow!("saving entry to log: {err:#}"));
        }
        self.peers[self.id].match_idx = self.storage.last_log_idx();

        // the transaction was inserted to log and will be sent to all followers in next iteration of leader loop
        // following future will wait for it to be commited and will respond to client
        let (tx, mut rx) = oneshot::channel();
        self.pending_transactions
            .insert(self.storage.last_log_idx(), tx);
        tokio::spawn(async move {
            select! {
                // TODO: process client disconnects via cancellation tokens,
                // otherwise we will be leaking memory in self.pending_transactions
                tx_result = &mut rx => {
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
        });

        debug!(index = self.last_applied_idx, "wrote new entry");
        Ok(())
    }

    async fn maybe_apply_log(&mut self) -> Result<bool> {
        if self.commit_idx <= self.last_applied_idx {
            return Ok(false);
        }
        self.last_applied_idx += 1;
        debug_assert!(
            self.last_applied_idx <= self.storage.last_log_idx(),
            "(applying log entries) last_applied_idx > log last idx"
        );

        debug!(index = self.last_applied_idx, "applying log entry");
        let cmd = self
            .storage
            .get_log_entry(self.last_applied_idx)
            .await
            .context("fetching latest log entry")?
            .command
            .clone()
            .unwrap();
        let old_value = self
            .storage
            .set(cmd.key, cmd.value)
            .await
            .context("applyng log entry to storage")?;
        if let Some(chan) = self.pending_transactions.remove(&self.last_applied_idx) {
            let _ = chan.send(old_value);
        }
        Ok(true)
    }

    async fn maybe_update_followers(&mut self) -> Result<()> {
        let last_log_idx = self.storage.last_log_idx();
        for peer_id in 0..self.peers.len() {
            if peer_id == self.id {
                continue;
            }

            let peer = &mut self.peers[peer_id];
            let ok_to_update = peer.update_timeout.map_or(true, |x| Instant::now() >= x);
            if !ok_to_update {
                continue;
            }

            if peer.next_idx <= last_log_idx {
                // peer is lagging
                debug!(
                    follower = peer_id,
                    my_last_log_idx = last_log_idx,
                    follower_next_idx = peer.next_idx,
                    "updating follower"
                );
                let entries = self
                    .storage
                    .get_logs_since(peer.next_idx)
                    .await
                    .context("getting entries for follower")?;
                self.send_entries(peer_id, entries)
                    .await
                    .context("sending entries to follower")?;
            } else if Instant::now() >= peer.next_heartbeat {
                self.send_entries(peer_id, Vec::new())
                    .await
                    .context("sending heartbeat")?;
            }
        }
        Ok(())
    }

    async fn maybe_update_commit_idx(&mut self) -> Result<()> {
        debug_assert!(
            self.commit_idx <= self.storage.last_log_idx(),
            "(updating commitIndex) commitIndex <= log len"
        );
        if self.commit_idx == self.storage.last_log_idx() {
            return Ok(());
        }
        let mut i = self.storage.last_log_idx();
        while i > self.commit_idx {
            let in_sync = self.peers.iter().filter(|p| p.match_idx >= i).count();
            if in_sync >= self.quorum as usize
                && self
                    .storage
                    .get_log_entry(i)
                    .await
                    .context("getting log entry")?
                    .term
                    == self.storage.current_term()
            {
                debug!(index = i, in_sync = in_sync, "got quorum on log entry");
                self.commit_idx = i;
                return Ok(());
            }
            i -= 1;
        }
        Ok(())
    }

    fn init_peers(addrs: Vec<String>) -> Result<Vec<PeerState>> {
        let mut peers = Vec::with_capacity(addrs.len());
        for addr in addrs.into_iter() {
            println!("connected to {}", addr);
            let ch = Channel::from_shared(addr)?.connect_lazy();
            peers.push(PeerState {
                client: SraftClient::new(ch),
                next_idx: 0,
                match_idx: 0,
                update_timeout: None,
                next_heartbeat: Instant::now(),
            });
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
    use storage::Store;

    use super::*;

    pub struct VolatileStore {
        log: Vec<grpc::LogEntry>,
        data: HashMap<String, Vec<u8>>,
        voted_for: Option<usize>,
        current_term: u64,
    }
    impl VolatileStore {
        pub(crate) fn new() -> Self {
            Self {
                log: Vec::new(),
                data: HashMap::new(),
                voted_for: None,
                current_term: 0,
            }
        }
    }

    impl Store for VolatileStore {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.data.get(key).map(|x| x.clone()))
        }

        async fn set(&mut self, key: String, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
            Ok(self.data.insert(key, value))
        }

        fn last_log_idx(&self) -> usize {
            self.log.len()
        }
        fn last_log_term(&self) -> u64 {
            self.log.last().map(|x| x.term).unwrap_or(0)
        }
        fn voted_for(&self) -> Option<usize> {
            self.voted_for
        }
        async fn reset_voted_for(&mut self) -> Result<()> {
            self.voted_for = None;
            Ok(())
        }
        async fn set_voted_for(&mut self, peer: usize) -> Result<()> {
            self.voted_for = Some(peer);
            Ok(())
        }
        fn current_term(&self) -> u64 {
            self.current_term
        }
        async fn set_current_term(&mut self, term: u64) -> Result<()> {
            self.current_term = term;
            Ok(())
        }
        async fn append_to_log(&mut self, entry: grpc::LogEntry) -> Result<()> {
            self.log.push(entry);
            Ok(())
        }
        async fn get_log_entry(&self, idx: usize) -> Result<grpc::LogEntry> {
            self.log
                .get(idx - 1)
                .map(|x| x.clone())
                .ok_or(anyhow!("wrong index"))
        }
        async fn get_logs_since(&self, start_idx: usize) -> Result<Vec<grpc::LogEntry>> {
            Ok(self.log[start_idx - 1..]
                .iter()
                .map(|x| x.clone())
                .collect())
        }
        async fn truncate_log(&mut self, start_idx: usize) -> Result<()> {
            self.log.truncate(start_idx);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_append_entries() {
        let (tx, rx) = mpsc::channel(1);
        let store = VolatileStore::new();
        let mut sm = StateMachine::new(0, Vec::new(), store, rx, tx).unwrap();
        sm.set_term(2).await.unwrap();
        let mut req = grpc::AppendEntriesRequest {
            term: 1,
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };

        // stale term
        let mut resp = sm.append_entries(req.clone()).await.unwrap();
        assert!(!resp.success);

        // ok term, no entries
        req.term = 2;
        resp = sm.append_entries(req.clone()).await.unwrap();
        assert!(resp.success);
        assert_eq!(sm.storage.last_log_idx(), 0);

        // insert entry
        req.leader_commit = 1;
        req.entries.push(grpc::LogEntry {
            term: 2,
            command: Some(grpc::Command {
                key: "foo".to_string(),
                value: "chlos".into(),
            }),
        });
        resp = sm.append_entries(req.clone()).await.unwrap();
        assert!(resp.success);
        assert_eq!(sm.storage.last_log_idx(), 1);
        assert_eq!(
            sm.storage
                .get_log_entry(1)
                .await
                .unwrap()
                .command
                .unwrap()
                .key,
            "foo"
        );
        assert_eq!(sm.commit_idx, 1);

        // another entry
        req.leader_commit = 2;
        req.prev_log_index = 1;
        req.prev_log_term = 2;
        resp = sm.append_entries(req.clone()).await.unwrap();
        assert!(resp.success);
        assert_eq!(sm.storage.last_log_idx(), 2);
        assert_eq!(sm.commit_idx, 2);

        // TODO: overwrite conflicting entries
        sm.storage
            .append_to_log(grpc::LogEntry {
                term: 42,
                command: Some(grpc::Command {
                    key: "wrong".to_string(),
                    value: "wrong".into(),
                }),
            })
            .await
            .unwrap();
        sm.commit_idx = 3;
        req.leader_commit = 3;
        req.prev_log_index = 2;
        resp = sm.append_entries(req.clone()).await.unwrap();
        assert!(resp.success);
        assert_eq!(sm.storage.last_log_idx(), 3);
        assert_eq!(sm.commit_idx, 3);
        assert_eq!(
            sm.storage
                .get_log_entry(2)
                .await
                .unwrap()
                .command
                .unwrap()
                .key,
            "foo"
        );
    }
}
