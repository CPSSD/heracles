use std::net::SocketAddr;
use std::sync::Arc;

use tokio_core::reactor::Core;
// use futures::{future, Stream};
use futures::Stream;

use super::*;
use operations::OperationHandler;

use cerberus_proto::datatypes as dpb;
use cerberus_proto::worker as wpb;

/// BrokerAdapter is used to make the new broker conform with the requirements of the ol worker
/// code.
pub struct BrokerAdapter;

impl BrokerAdapter {
    pub fn run(broker_addr: SocketAddr, operation_handler: Arc<OperationHandler>) {
        let broker = Broker::connect(broker_addr, Core::new().unwrap().handle()).unwrap();

        broker.handle.map_err(|_| unimplemented!("receiver should not error"))
            .for_each(move |task| {
                match convert_to_old_task(task) {
                    OldTask::Map(old_task) => {
                        operation_handler.perform_map(old_task)
                    }
                    OldTask::Reduce(old_task) => {
                        operation_handler.perform_reduce(old_task)
                    }
                }
            });
    }
}

enum OldTask {
    Map(wpb::PerformMapRequest),
    Reduce(wpb::PerformReduceRequest),
}

fn convert_to_old_task(new_task: dpb::Task) -> OldTask {
    match new_task.field_type {
        dpb::TaskType::MAP => {
            let mut task = wpb::PerformMapRequest::new();
            task.mapper_file_path = new_task.payload_path;
            if !new_task.input_files.is_empty(){
                task.input_file_path = new_task.input_files.first().unwrap().path;
            }
            OldTask::Map(task)
        }
        dpb::TaskType::REDUCE => {
            let mut task = wpb::PerformReduceRequest::new();
            task.reducer_file_path = new_task.payload_path;
            OldTask::Reduce(task)
        }
    }
}
