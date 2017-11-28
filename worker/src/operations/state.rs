use std::path::PathBuf;

use cerberus_proto::worker as pb;

/// `OperationState` is a data only struct for holding the current state for the `OperationHandler`
#[derive(Default)]
pub struct OperationState {
    pub worker_status: pb::WorkerStatus,
    pub operation_status: pb::OperationStatus,

    // Initial CPU time of the current operation. This is used to calculate the total cpu time used
    // for an operation.
    pub initial_cpu_time: u64,

    pub intermediate_file_store: Vec<PathBuf>,
}

impl OperationState {
    pub fn new() -> Self {
        OperationState {
            worker_status: pb::WorkerStatus::AVAILABLE,
            operation_status: pb::OperationStatus::UNKNOWN,
            initial_cpu_time: 0,
            intermediate_file_store: Vec::new(),
        }
    }

    pub fn add_intermediate_files(&mut self, files: Vec<PathBuf>) {
        self.intermediate_file_store.extend(files.into_iter());
    }
}