mod file;

use self::file::FileStore;

use std::fmt;
use std::fmt::Display;

use failure::*;

use heracles_proto::datatypes::Task;

#[allow(doc_markdown)]
pub trait State {
    /// Updates the task status. If the task.Status is marked as DONE, the task is also removed
    /// from the list of pending tasks.
    fn save_progress(&self, task: &Task) -> Result<(), StateError>;
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum StateErrorKind {
    #[fail(display = "Failed to connect to state store server.")]
    ConnectionFailed,
    #[fail(display = "Task is missing in the state store")]
    MissingTask,
    #[fail(display = "Pending task is missing from the state store")]
    MissingPendingTask,
    #[fail(display = "Failed to serialise the task proto.")]
    TaskSerialisationFailed,
    #[fail(display = "Failed to deserialise the task proto.")]
    TaskDeserialisationFailed,
    #[fail(display = "Failed to write task")]
    TaskWriteFailed,
    #[fail(display = "Failed to remove pending task")]
    RemovingPendingTaskFailed,
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
