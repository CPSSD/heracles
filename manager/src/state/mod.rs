mod file;

use self::file::FileStore;

use std::fmt;
use std::fmt::Display;

use failure::*;
use futures::Future;

use heracles_proto::datatypes::{Job, Task, TaskKind};

#[allow(doc_markdown)]
/// Interface for creating connections to state stores, such as etcd or TiKV etc.
pub trait State {
    /// Serialize the job and save it in the state store so it can be loaded later.
    fn save_job(&self, job: &Job) -> Result<(), StateError>;
    /// Adds a task to the list of tasks and add it to pending
    fn save_task(&self, task: &Task) -> Result<(), StateError>;
    /// List of pending map tasks for a specific job.
    fn pending_map_tasks(&self, job: &Job) -> Result<Vec<Task>, StateError>;
    /// List of pending reduce tasks.
    fn pending_reduce_tasks(&self, job: &Job) -> Result<Vec<Task>, StateError>;
    /// Returns a future when all map tasks are done.
    fn map_done(&self, job: &Job) -> Box<Future<Item = (), Error = StateError>>;
    /// Returns a future when all reduce tasks are done.
    fn reduce_done(&self, job: &Job) -> Box<Future<Item = (), Error = StateError>>;
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum StateErrorKind {
    #[fail(display = "Failed to connect to state store server.")]
    ConnectionFailed,
    #[fail(display = "Failed precondition")]
    PreconditionFailed,
    #[fail(display = "Unable to create required jobs folder")]
    JobsFolderCreationFailed,
    #[fail(display = "Unable to remove jobs folder")]
    JobsFolderRemoveFailed,
    #[fail(display = "Unable to create required tasks folder")]
    TasksFolderCreationFailed,
    #[fail(display = "Unable to create required map tasks folder")]
    MapTasksFolderCreationFailed,
    #[fail(display = "Unable to create required reduce tasks folder")]
    ReduceTasksFolderCreationFailed,
    #[fail(display = "Unable to list pending tasks")]
    PendingTasksListFailed,
    #[fail(display = "An unknown I/O error has occurred.")]
    GenericIOError,
    #[fail(display = "Failed to serialise the job proto.")]
    JobSerialisationFailed,
    #[fail(display = "Failed to serialise the task proto.")]
    TaskSerialisationFailed,
    #[fail(display = "Failed to deserialise the task proto.")]
    TaskDeserialisationFailed,
    #[fail(display = "Failed to write task")]
    JobWriteFailed,
    #[fail(display = "Unable to open task file")]
    TaskFileOpenFailed,
    #[fail(display = "Failed to write task")]
    TaskWriteFailed,
    #[fail(display = "Failed to create pending task")]
    PendingTaskWriteFailed,
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
