use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
pub struct PeerCfg {
    pub id: u32,
    pub addr: String,
}

#[derive(Deserialize)]
pub struct Config {
    pub peer_id: u32,
    pub host: Option<String>,
    pub port: u16,
    pub peers: Vec<PeerCfg>,
}

impl Config {
    pub fn new(file: &str) -> Result<Self> {
        let cfg: Config = toml::from_str(&fs::read_to_string(file)?)?;
        Ok(cfg)
    }
}
