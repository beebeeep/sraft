use grpc::{
    set_response::PreviousValue, sraft_server::Sraft, AppendEntriesRequest, AppendEntriesResponse,
    GetRequest, GetResponse, RequestVoteRequest, RequestVoteResponse, SetRequest, SetResponse,
};
use tonic::{Request, Response, Status};
use tracing::{error, info};

pub mod grpc {
    tonic::include_proto!("sraft");
}

#[tonic::async_trait]
impl Sraft for super::SraftNode {
    async fn append_entries(
        &self,
        req: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = req.into_inner();
        match self.append_entries(req.clone()).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(err) => {
                error!(
                    term = req.term,
                    error = format!("{err:#}"),
                    "AppendEntries failed"
                );
                Err(Status::internal(format!("{err:#}")))
            }
        }
    }

    async fn request_vote(
        &self,
        req: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = req.into_inner();
        match self.request_vote(req.clone()).await {
            Ok(resp) => {
                info!(
                    candidate = req.candidate_id,
                    voted = resp.vote_granted,
                    "voted"
                );
                Ok(Response::new(resp))
            }
            Err(err) => {
                error!(
                    candidate_id = req.candidate_id,
                    term = req.term,
                    error = format!("{err:#}"),
                    "RequestVote failed"
                );
                Err(Status::internal(format!("{err:#}")))
            }
        }
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
                previous_value: v.map(PreviousValue::Value),
            })),
            Err(err) => Err(Status::internal(format!("setting key: {err:#}"))),
        }
    }
}
