//! Module for splitting a `Job` into a set of Reduce `Task`s.
//!
//! There is one Reduce `Task` per desired output file.

use chrono::Utc;
use uuid::Uuid;

use cerberus_proto::datatypes::*;

pub fn split(job: &Job) -> Vec<Task> {
    job.get_output_files()
        .iter()
        .map(|file| {
            let mut task = Task::new();
            task.set_id(Uuid::new_v4().to_string());
            task.set_job_id(job.get_id().to_string());
            task.set_status(TaskStatus::TASK_PENDING);
            task.set_kind(TaskKind::REDUCE);
            task.set_time_created(Utc::now().timestamp() as u64);
            task.set_output_file(file.to_string());
            task.set_payload_path(job.get_payload_path().to_string());
            task
        })
        .collect()
}
