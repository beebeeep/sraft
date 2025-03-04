use std::time::Instant;

use anyhow::Result;
use clap::Parser;
use rand::{Rng, RngCore};
use sraft::sraft::api::grpc::{self, sraft_client::SraftClient};
use tonic::transport::Channel;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    addr: String,
    #[arg(short, long)]
    count: u64,
    #[arg(short, long)]
    payload_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let ch = Channel::from_shared(args.addr)?.connect().await?;
    let mut client = SraftClient::new(ch);
    let mut rng = rand::rng();
    for _ in 0..args.count {
        let t = Instant::now();
        let mut req = grpc::SetRequest {
            key: format!("{}", rng.random::<u32>()),
            value: Vec::with_capacity(args.payload_size),
        };
        rng.fill_bytes(&mut req.value);
        client.set(req).await?;
        println!("request took {}us", (Instant::now() - t).as_micros());
    }
    println!("hello world");
    Ok(())
}
