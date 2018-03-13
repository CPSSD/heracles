use std::fmt;
use std::fmt::Display;

use failure::*;
use futures::Future;

use cerberus_proto::datatypes::{Job, Task};

#[allow(doc_markdown)]
/// Interface for creating connections to state stores, such as etcd or TiKV etc.
pub trait State {
    /// Marks a specific job as owned by a certain scheduler.
    fn mark_job_scheduler(job: &Job) -> Result<(), StateError>;
    /// Get a list of unfinished jobs
    fn get_unfinished_jobs() -> Result<Vec<Job>, StateError>;
    /// List of jobs assigned to current scheduler
    fn list_scheduler_jobs() -> Result<Vec<Job>, StateError>;
    /// Serialize the job and save it in the state store so it can be loaded later.
    fn save_job(job: &Job) -> Result<(), StateError>;
    /// Adds a task to the list of tasks and add it to pending
    fn save_task(task: &Task) -> Result<(), StateError>;
    /// List of pending map tasks for a specific job.
    fn pending_map_tasks(job: &Job) -> Result<Vec<Task>, StateError>;
    /// List of pending reduce tasks.
    fn pending_reduce_tasks(job: &Job) -> Result<Vec<Task>, StateError>;
    /// Returns a future when all map tasks are done.
    fn map_done(job: &Job) -> Future<Item = (), Error = StateError>;
    /// Returns a future when all reduce tasks are done.
    fn reduce_done(job: &Job) -> Future<Item = (), Error = StateError>;
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum StateErrorKind {
    #[fail(display = "Failed to connect to state store server.")]
    ConnectionFailed,
    #[fail(display = "Failed to serialize the job proto.")]
    JobSerialisationFailed,
    #[fail(display = "Failed operation.")]
    OperationFailed,
}

#[derive(Debug)]
pub struct StateError {
    inner: Context<StateErrorKind>,
}

impl StateError {
    pub fn kind(&self) -> StateErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for StateError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl From<StateErrorKind> for StateError {
    fn from(kind: StateErrorKind) -> StateError {
        StateError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<StateErrorKind>> for StateError {
    fn from(inner: Context<StateErrorKind>) -> StateError {
        StateError { inner }
    }
}
