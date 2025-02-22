use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use rand::rng;
use rand::Rng;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time;
use tokio::time::Instant;
use tokio::time::Sleep;
use tonic::transport::Channel;

use super::api::grpc::sraft_client;
use super::api::grpc::sraft_client::SraftClient;
use super::api::grpc::LogEntry;

type PeerID = u64;

pub enum ServerState {
    Leader,
    Follower,
    Candidate,
}

pub enum Message {
    Get {
        key: String,
        resp: oneshot::Sender<Option<Vec<u8>>>,
    },
    Set {
        key: String,
        value: Vec<u8>,
        resp: oneshot::Sender<Option<Vec<u8>>>,
    },
}

pub struct StateMachine {
    id: PeerID,
    actions: mpsc::Receiver<Message>,
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
        peers: Vec<(PeerID, String)>,
        actions: mpsc::Receiver<Message>,
    ) -> Result<Self> {
        Ok(Self {
            id,
            actions,
            data: HashMap::new(),
            peers: connect_to_peers(peers)?,
            election_timeout: get_election_timeout(),

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
            // TODO: main loop, implement state transitions
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
            Some(msg) = self.actions.recv() => {
                self.handle_message(msg);
            }
        }
    }

    async fn run_candidate(&mut self) {
        todo!()
    }

    fn convert_to_candidate(&mut self) {
        self.state = ServerState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.election_timeout = get_election_timeout();
        for (id, client) in self.peers.iter() {
            todo!("send RequestVote to {id}")
        }
    }

    fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::Get { key, resp } => {
                let _ = resp.send(self.data.get(&key).cloned()); // TODO: storage can be shared with grpc layer probably?
            }
            Message::Set { key, value, resp } => {
                let _ = resp.send(self.data.insert(key, value));
            }
        }
    }

    fn maybe_apply_log(&mut self) -> bool {
        if self.commit_idx > self.last_applied_idx {
            self.last_applied_idx += 1;
            let entry = self.log[self.last_applied_idx].clone();
            self.data.insert(entry.key, entry.value);
            return true;
        }
        return false;
    }
}

fn connect_to_peers(addrs: Vec<(PeerID, String)>) -> Result<HashMap<PeerID, SraftClient<Channel>>> {
    let mut peers = HashMap::with_capacity(addrs.len());
    for addr in addrs {
        let ch = Channel::from_shared(addr.1)?.connect_lazy();
        peers.insert(addr.0, SraftClient::new(ch));
    }
    Ok(peers)
}

fn get_election_timeout() -> Instant {
    Instant::now()
        .checked_add(Duration::from_millis(rng().random_range(200..300)))
        .unwrap()
}
