use sraft::sraft::{api::grpc::sraft_server::SraftServer, SraftNode};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let node = SraftNode::new();

    Server::builder()
        .add_service(SraftServer::new(node))
        .serve(addr)
        .await?;

    Ok(())
}
