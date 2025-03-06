use anyhow::{Context, Result};
use rusqlite::Connection;
use tokio::task;

pub struct NodeStorage {
    db: Connection,
    last_log_idx: u64,
}

impl NodeStorage {
    fn new(db_path: &str) -> Result<NodeStorage> {
        let db = Connection::open(db_path).context(format!("opening {db_path}"))?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS store(
                key TEXT PRIMARY KEY, value BLOB, created_ts INTEGER, updated_ts INTEGER
        )",
            (),
        )
        .context("creating schema")?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS log(
                idx INTEGER PRIMARY KEY, term INTEGER, key TEXT, value BLOB
        )",
            (),
        )
        .context("creating schema")?;
        db.execute(
            "CREATE TABLE IF NOT EXIST metadata(
                current_term INTEGER, voted_for INTEGER",
            (),
        )
        .context("creating schema")?;

        let last_log_idx = db
            .query_row("SELECT max(idx) FROM log", [], |r| r.get(0))
            .context("querying log")?;
        Ok(NodeStorage { db, last_log_idx })
    }

    async fn get(key: &str) -> Result<Option<Vec<u8>>> {
        task::spawn(future)
    }
}
