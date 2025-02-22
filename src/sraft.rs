use std::{collections::HashMap, sync::Arc};

use api::grpc::LogEntry;
use tokio::sync::RwLock;

pub mod api;

#[derive(Default)]
pub struct SraftNode {
    current_term: u64,
    voted_for: u32,
    log: Vec<LogEntry>,
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}
