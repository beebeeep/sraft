use std::collections::HashMap;

use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::api::grpc::LogEntry;

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
    actions: mpsc::Receiver<Message>,
    current_term: u64,
    voted_for: Option<u32>,
    log: Vec<LogEntry>,
    state: ServerState,
    data: HashMap<String, Vec<u8>>,
}

impl StateMachine {
    pub fn new(actions: mpsc::Receiver<Message>) -> Self {
        Self {
            actions,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            state: ServerState::Follower,
            data: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        loop {
            // TODO: main loop, implement state transitions
            select! {
                Some(msg) = self.actions.recv() => {
                    self.handle_message(msg);
                }
            }
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
}
