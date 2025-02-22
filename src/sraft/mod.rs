use anyhow::Result;
use state_machine::{Message, StateMachine};
use tokio::sync::{mpsc, oneshot};

pub mod api;
mod state_machine;

pub struct SraftNode {
    actions: mpsc::Sender<Message>,
}

impl SraftNode {
    pub fn new() -> Self {
        let (send, recv) = mpsc::channel(8);

        let mut sm = StateMachine::new(recv);
        tokio::spawn(async move { sm.run().await });

        SraftNode { actions: send }
    }

    async fn get(&self, key: String) -> Result<Option<Vec<u8>>> {
        let (send, recv) = oneshot::channel();
        let msg = Message::Get {
            key: key,
            resp: send,
        };
        let _ = self.actions.send(msg).await;
        Ok(recv.await?)
    }

    async fn set(&self, key: String, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let (send, recv) = oneshot::channel();
        let msg = Message::Set {
            key: key,
            value: value,
            resp: send,
        };
        let _ = self.actions.send(msg).await;
        Ok(recv.await?)
    }
}
