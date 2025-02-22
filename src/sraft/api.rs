use grpc::{
    set_response::PreviousValue, sraft_server::Sraft, AppendEntriesRequest, AppendEntriesResponse,
    GetRequest, GetResponse, RequestVoteRequest, RequestVoteResponse, SetRequest, SetResponse,
};
use tonic::{Request, Response, Status};

pub mod grpc {
    tonic::include_proto!("sraft");
}

#[tonic::async_trait]
impl Sraft for super::SraftNode {
    async fn append_entries(
        &self,
        _req: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        todo!("nopers");
    }

    async fn request_vote(
        &self,
        _req: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        todo!("nopers");
    }

    async fn get(&self, req: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let d = self.data.read().await;
        match d.get(&req.into_inner().key) {
            Some(v) => Ok(Response::new(GetResponse { value: v.to_vec() })),
            None => Err(Status::not_found("key not found")),
        }
    }

    async fn set(&self, req: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let r = req.into_inner();
        let mut d = self.data.write().await;
        match d.insert(r.key, r.value) {
            Some(v) => Ok(Response::new(SetResponse {
                previous_value: Some(PreviousValue::Value(v)),
            })),
            None => Ok(Response::new(SetResponse {
                previous_value: None,
            })),
        }
    }
}
