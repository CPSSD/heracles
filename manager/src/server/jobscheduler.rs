use grpc::{RequestOptions, SingleResponse};

use super::*;
use heracles_proto::mapreduce as pb;
use heracles_proto::mapreduce_grpc as grpc_pb;
use scheduler::Scheduler;

// use std::sync::Arc;

pub struct JobScheduleService {
    scheduler: Scheduler,
}

impl JobScheduleService {
    pub fn new(scheduler: Scheduler) -> Self {
        JobScheduleService { scheduler }
    }
}

impl grpc_pb::JobScheduleService for JobScheduleService {
    fn schedule(
        &self,
        _: RequestOptions,
        req: pb::ScheduleRequest,
    ) -> SingleResponse<pb::ScheduleResponse> {
        match self.scheduler.schedule(req.get_job()) {
            Ok(job_id) => {
                let mut res = pb::ScheduleResponse::new();
                res.set_job_id(job_id);
                return SingleResponse::completed(res);
            }
            Err(err) => {
                error!("{}", err);
                return SingleResponse::err(grpc::Error::Other(""));
            }
        }
    }

    fn cancel(
        &self,
        _: RequestOptions,
        req: pb::CancelRequest,
    ) -> SingleResponse<pb::EmptyMessage> {
        match self.scheduler.cancel(req.get_job_id()) {
            Ok(_) => {
                return SingleResponse::completed(pb::EmptyMessage::new());
            }
            Err(err) => {
                error!("{}", err);
                return SingleResponse::err(grpc::Error::Other(""));
            }
        }
    }

    fn describe(
        &self,
        _: RequestOptions,
        req: pb::DescribeRequest,
    ) -> SingleResponse<pb::Description> {
        unimplemented!()
    }
}
