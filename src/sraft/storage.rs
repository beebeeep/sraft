use anyhow::{anyhow, Context, Result};
use prost::Message;
use tokio::task;
use tracing::debug;

use super::api::grpc;

const KEY_VOTED_FOR: &'static str = "voted_for";
const KEY_CURRENT_TERM: &'static str = "current_term";

pub(super) trait Store {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&mut self, key: String, value: Vec<u8>) -> Result<Option<Vec<u8>>>;
    fn last_log_idx(&self) -> usize;
    fn last_log_term(&self) -> u64;
    fn voted_for(&self) -> Option<usize>;
    async fn reset_voted_for(&mut self) -> Result<()>;
    async fn set_voted_for(&mut self, peer: usize) -> Result<()>;
    fn current_term(&self) -> u64;
    async fn set_current_term(&mut self, term: u64) -> Result<()>;
    async fn append_to_log(&mut self, entry: grpc::LogEntry) -> Result<()>;
    async fn get_log_entry(&self, idx: usize) -> Result<grpc::LogEntry>;
    async fn get_logs_since(&self, start_idx: usize) -> Result<Vec<grpc::LogEntry>>;
    async fn truncate_log(&mut self, start_idx: usize) -> Result<()>;
}

pub struct PersistentStore {
    _keyspace: fjall::Keyspace,
    storage: fjall::PartitionHandle,
    log: fjall::PartitionHandle,
    metadata: fjall::PartitionHandle,

    last_log_idx: usize,
    last_log_term: u64,
    voted_for: Option<usize>,
    current_term: u64,
}

impl PersistentStore {
    pub(super) fn new(db_dir: &str) -> Result<Self> {
        let keyspace = fjall::Config::new(db_dir).open().context("opening db")?;
        let storage = keyspace
            .open_partition("storage", fjall::PartitionCreateOptions::default())
            .context("opening storage partition")?;
        let log = keyspace
            .open_partition("log", fjall::PartitionCreateOptions::default())
            .context("opening log keyspace")?;
        let metadata = keyspace
            .open_partition("metadata", fjall::PartitionCreateOptions::default())
            .context("opening metadata keyspace")?;

        let (last_entry, last_log_idx) =
            Self::check_log(&log).context("checking log consistency")?;
        let mut buf = [0; 8];
        let voted_for = metadata
            .get(KEY_VOTED_FOR)
            .context("getting voted_for")?
            .map(|s| {
                buf.copy_from_slice(&s);
                usize::from_be_bytes(buf)
            });
        let current_term = metadata
            .get(KEY_VOTED_FOR)
            .context("getting voted_for")?
            .map(|s| {
                buf.copy_from_slice(&s);
                u64::from_be_bytes(buf)
            })
            .unwrap_or(0);

        debug!(
            log_length = log.approximate_len(),
            data_length = storage.approximate_len(),
            "initialized persistent storage"
        );
        Ok(Self {
            _keyspace: keyspace,
            storage,
            log,
            metadata,
            last_log_idx,
            last_log_term: last_entry.term,
            voted_for,
            current_term,
        })
    }

    // check_log goes through whole log and checks two invariants:
    // each subsequent log entry has term greater or equal than preivous one
    // each log entry has index matching its position in storage, counting from 1
    // returns last log entry and its index
    fn check_log(log: &fjall::PartitionHandle) -> Result<(grpc::LogEntry, usize)> {
        let mut prev_term = 0;
        let mut last_idx = 0;
        let mut buf = [0; 8];
        let mut entry = grpc::LogEntry::default();
        for (idx, kv) in log.iter().enumerate() {
            match kv {
                Ok((ks, vs)) => {
                    entry = grpc::LogEntry::decode(vs.as_ref()).context("decoding message")?;

                    if entry.term < prev_term {
                        return Err(anyhow!("invalid log entry at {idx}: term {} is less than in term of previous entries", entry.term));
                    }
                    prev_term = entry.term;
                    buf.copy_from_slice(&ks);
                    let entry_idx = usize::from_be_bytes(buf);
                    if entry_idx != idx + 1 {
                        return Err(anyhow!("log entry with index {entry_idx} is in unexpected position {idx} in storage"));
                    }
                    last_idx = entry_idx;
                }
                Err(err) => {
                    return Err(anyhow!("reading log entry from storage: {err}"));
                }
            }
        }
        Ok((entry, last_idx))
    }
}

impl Store for PersistentStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let storage = self.storage.clone();
        let key = key.to_string();
        let data = task::spawn_blocking(move || storage.get(key))
            .await
            .context("reading from storage")??;
        Ok(data.map(|r| r.to_vec()))
    }

    async fn set(&mut self, key: String, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
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

    fn last_log_term(&self) -> u64 {
        self.last_log_term
    }

    fn voted_for(&self) -> Option<usize> {
        self.voted_for
    }

    async fn reset_voted_for(&mut self) -> Result<()> {
        let metadata = self.metadata.clone();
        task::spawn_blocking(move || metadata.remove(KEY_VOTED_FOR)).await??;
        self.voted_for = None;
        Ok(())
    }

    async fn set_voted_for(&mut self, peer: usize) -> Result<()> {
        let metadata = self.metadata.clone();
        task::spawn_blocking(move || metadata.insert(KEY_VOTED_FOR, peer.to_be_bytes()))
            .await
            .context("saving_to_storage")??;
        self.voted_for = Some(peer);
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

    async fn append_to_log(&mut self, entry: grpc::LogEntry) -> Result<()> {
        let log = self.log.clone();
        let next_idx = self.last_log_idx + 1;
        let last_log_term = entry.term;
        task::spawn_blocking(move || log.insert(next_idx.to_be_bytes(), entry.encode_to_vec()))
            .await
            .context("writing to storage")??;
        debug!(
            idx = self.last_log_idx + 1,
            term = last_log_term,
            "added entry to log"
        );
        self.last_log_idx += 1;
        self.last_log_term = last_log_term;
        Ok(())
    }

    async fn get_log_entry(&self, idx: usize) -> Result<grpc::LogEntry> {
        let log = self.log.clone();
        task::spawn_blocking(move || {
            match log.get(idx.to_be_bytes()).context("reading storage")? {
                Some(e) => {
                    let e = grpc::LogEntry::decode(e.as_ref()).context("decoding log entry")?;
                    Ok(e)
                }
                None => Err(anyhow!("no log entry at {idx}")),
            }
        })
        .await?
    }

    async fn get_logs_since(&self, start_idx: usize) -> Result<Vec<grpc::LogEntry>> {
        let mut result = Vec::with_capacity(self.last_log_idx - start_idx);
        for idx in start_idx..=self.last_log_idx {
            result.push(self.get_log_entry(idx).await?);
        }
        Ok(result)
    }

    async fn truncate_log(&mut self, start_idx: usize) -> Result<()> {
        let log = self.log.clone();
        let last_idx = self.last_log_idx;
        let last_term = task::spawn_blocking(move || -> Result<u64> {
            for idx in start_idx..=last_idx {
                log.remove(idx.to_be_bytes())
                    .context("removing log entry")?;
            }
            if let Some(e) = log
                .get((start_idx - 1).to_be_bytes())
                .context("reading log entry")?
            {
                let entry = grpc::LogEntry::decode(e.as_ref()).context("decoding log entry")?;
                Ok(entry.term)
            } else {
                Ok(0)
            }
        })
        .await??;
        self.last_log_idx = start_idx - 1;
        self.last_log_term = last_term;
        Ok(())
    }
}
