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
        match self.get(req.into_inner().key).await {
            Ok(Some(v)) => Ok(Response::new(GetResponse { value: v.to_vec() })),
            Ok(None) => Err(Status::not_found("key not found")),
            Err(err) => Err(Status::internal(format!("getting key: {err:#}"))),
        }
    }

    async fn set(&self, req: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = req.into_inner();
        match self.set(req.key, req.value).await {
            Ok(v) => Ok(Response::new(SetResponse {
                previous_value: v.map(|x| PreviousValue::Value(x)),
            })),
            Err(err) => Err(Status::internal(format!("setting key: {err:#}"))),
        }
    }
}
