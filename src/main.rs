use std::fs;

use clap::Parser;
use sraft::{
    config,
    sraft::{api::grpc::sraft_server::SraftServer, SraftNode},
};
use tonic::transport::Server;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[arg(short, long, default_value = "debug")]
    log_level: String,
    #[arg(short = 'f', long)]
    log_file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let cfg = config::Config::new(&args.config)?;

    let layer = tracing_subscriber::fmt::layer().compact();
    let filter = EnvFilter::builder().parse(format!("info,sraft={}", args.log_level))?;
    let subscriber = tracing_subscriber::registry().with(layer).with(filter);
    if let Some(file) = args.log_file {
        let layer = tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(false)
            .with_writer(
                fs::OpenOptions::new()
                    .truncate(true)
                    .write(true)
                    .open(file)?,
            );
        tracing::subscriber::set_global_default(subscriber.with(layer))?;
    } else {
        tracing::subscriber::set_global_default(subscriber)?;
    }

    let addr = format!(
        "{}:{}",
        cfg.host.clone().unwrap_or(String::from("[::]")),
        cfg.port
    )
    .parse()?;
    let node = SraftNode::new(&cfg)?;

    Server::builder()
        .add_service(SraftServer::new(node))
        .serve(addr)
        .await?;

    Ok(())
}
