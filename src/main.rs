use clap::Parser;
use sraft::{
    config,
    sraft::{api::grpc::sraft_server::SraftServer, SraftNode},
};
use tonic::transport::Server;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let layer = tracing_subscriber::fmt::layer().compact();
    let subscriber = tracing_subscriber::registry()
        .with(layer)
        .with(tracing_subscriber::EnvFilter::from_default_env());
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    let cfg = config::Config::new(&args.config)?;

    let addr = format!(
        "{}:{}",
        cfg.host.clone().unwrap_or(String::from("[::1]")),
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
