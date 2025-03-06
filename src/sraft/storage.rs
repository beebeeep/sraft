use core::slice::SlicePattern;

use anyhow::{Context, Result};
use tokio::task;

use super::api::grpc;

const KEY_VOTED_FOR: &'static str = "voted_for";
const KEY_CURRENT_TERM: &'static str = "current_term";

pub struct NodeStorage {
    keyspace: fjall::Keyspace,
    storage: fjall::PartitionHandle,
    log: fjall::PartitionHandle,
    metadata: fjall::PartitionHandle,

    last_log_idx: usize,
    voted_for: usize,
    current_term: u64,
}

impl NodeStorage {
    fn new(db_path: &str) -> Result<Self> {
        let keyspace = fjall::Config::new(db_path).open().context("opening db")?;
        let storage = keyspace
            .open_partition("storage", fjall::PartitionCreateOptions::default())
            .context("opening storage partition")?;
        let log = keyspace
            .open_partition("log", fjall::PartitionCreateOptions::default())
            .context("opening log keyspace")?;
        let metadata = keyspace
            .open_partition("metadata", fjall::PartitionCreateOptions::default())
            .context("opening metadata keyspace")?;

        let last_log_idx = log.len().context("getting last log index")?;
        let mut buf = [0; 8];
        let voted_for = metadata
            .get(KEY_VOTED_FOR)
            .context("getting voted_for")?
            .map(|s| {
                buf.copy_from_slice(&s);
                usize::from_be_bytes(buf)
            })
            .unwrap_or(0);
        let current_term = metadata
            .get(KEY_VOTED_FOR)
            .context("getting voted_for")?
            .map(|s| {
                buf.copy_from_slice(&s);
                u64::from_be_bytes(buf)
            })
            .unwrap_or(0);

        Ok(Self {
            keyspace,
            storage,
            log,
            metadata,
            last_log_idx,
            voted_for,
            current_term,
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let storage = self.storage.clone();
        let key = key.to_string();
        let data = task::spawn_blocking(move || storage.get(key))
            .await
            .context("reading from storage")??;
        Ok(data.map(|r| r.to_vec()))
    }

    async fn set(&self, key: String, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let storage = self.storage.clone();
        let prev_value = task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
            let mut prev_value = None;
            if let Some(v) = storage.get(&key).context("reading from storage")? {
                prev_value = Some(v.to_vec())
            }
            storage.insert(&key, value).context("writing to storage")?;
            Ok(prev_value)
        })
        .await??;
        Ok(prev_value)
    }

    fn last_log_idx(&self) -> usize {
        self.last_log_idx
    }

    fn get_voted_for(&self) -> usize {
        self.voted_for
    }

    async fn set_voted_for(&mut self, peer: usize) -> Result<()> {
        let metadata = self.metadata.clone();
        task::spawn_blocking(move || metadata.insert(KEY_VOTED_FOR, peer.to_be_bytes()))
            .await
            .context("saving_to_storage")??;
        self.voted_for = peer;
        Ok(())
    }

    fn current_term(&self) -> u64 {
        self.current_term
    }

    async fn set_current_term(&mut self, term: u64) -> Result<()> {
        let metadata = self.metadata.clone();
        task::spawn_blocking(move || metadata.insert(KEY_CURRENT_TERM, term.to_be_bytes()))
            .await
            .context("saving_to_storage")??;
        self.current_term = term;
        Ok(())
    }

    async fn append_to_log(&self, entry: grpc::LogEntry) -> Result<()> {
        todo!()
    }
}
