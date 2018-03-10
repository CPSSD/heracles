pub mod etcd;

pub use self::etcd::EtcdStore;

use std::fmt;
use std::fmt::Display;
use std::net::SocketAddr;

use failure::*;
use futures::Future;
use tokio_core::reactor::Handle;

use cerberus_proto::datatypes::Job;

#[allow(doc_markdown)]
/// Interface for creating connections to state stores, such as etcd or TiKV etc.
pub trait State {
    fn connect(addrs: Vec<SocketAddr>, handle: Handle) -> Result<(), StateError>;
    /// Future which can only return an error. Will not complete unless an error occurs
    /// in the connection to the state store.
    fn error_future() -> Box<Future<Item = (), Error = StateError>>;
    /// Serialize the job and save it in the state store so it can be loaded later.
    fn save_job(job: &Job) -> Result<(), StateError>;
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
