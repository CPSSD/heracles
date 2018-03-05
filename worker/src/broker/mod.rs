pub mod amqp;

pub use self::amqp::Amqp;

use std::fmt;
use std::fmt::Display;
use std::net::SocketAddr;

use failure::*;
use futures::Future;
use futures::sync::mpsc;
use tokio_core::reactor::Handle;

use cerberus_proto::datatypes;

#[allow(doc_markdown)]
/// Interface for creating connection to a message broker, such as RabbitMQ, ZeroMQ, etc.
pub trait Broker {
    fn connect(add: SocketAddr, handle: Handle) -> Result<BrokerConnection, BrokerError>;
}

/// Returned from [`Broker::connect`], representing a connection to a message broker.
pub struct BrokerConnection {
    /// Future which can only return an error. Will not complete unless an error occurs in the
    /// connection to the broker
    pub error_future: Box<Future<Item = (), Error = BrokerError>>,
    /// Receiver end of a channel used to recieve Tasks from the broker.
    /// All messages on this channel are recieved from the broker, serialized into
    /// [`Task`s](cerberus_proto::datatypes::Task) to be processed.
    pub handle: mpsc::Receiver<datatypes::Task>,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum BrokerErrorKind {
    #[fail(display = "Failed to connect to message broker server.")]
    ConnectionFailed,
    #[fail(display = "Failed to deserialise the Task proto.")]
    TaskDeserialisationFailed,
    #[fail(display = "Failed sending task to the channel: receiver has been dropped")]
    ChannelSendFailed,
}

#[derive(Debug)]
pub struct BrokerError {
    inner: Context<BrokerErrorKind>,
}

impl BrokerError {
    pub fn kind(&self) -> BrokerErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for BrokerError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl From<BrokerErrorKind> for BrokerError {
    fn from(kind: BrokerErrorKind) -> BrokerError {
        BrokerError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<BrokerErrorKind>> for BrokerError {
    fn from(inner: Context<BrokerErrorKind>) -> BrokerError {
        BrokerError { inner }
    }
}
