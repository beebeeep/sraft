use sraft::{
    node_server::{Node, NodeServer},
    AppendEntriesRequest, AppendEntriesResponce, RequestVoteRequest, RequestVoteResponce,
};
use tonic::{transport::Server, Request, Response, Status};

pub mod sraft {
    tonic::include_proto!("sraft");
}

#[derive(Default)]
pub struct NodeImpl {}

#[tonic::async_trait]
impl Node for NodeImpl {
    async fn append_entries(
        &self,
        req: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponce>, Status> {
        todo!("nopers");
    }

    async fn request_vote(
        &self,
        req: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponce>, Status> {
        todo!("nopers");
    }
}
