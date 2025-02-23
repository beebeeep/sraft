use clap::Parser;
use sraft::sraft::{api::grpc::sraft_server::SraftServer, SraftNode};
use tonic::transport::Server;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    id: u32,
    #[arg(short, long)]
    port: u16,
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = format!("[::1]:{args.port}").parse()?;
    let node = SraftNode::new();

    Server::builder()
        .add_service(SraftServer::new(node))
        .serve(addr)
        .await?;

    Ok(())
}
