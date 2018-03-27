pub mod amqp;

use std::fmt;
use std::fmt::Display;

use failure::*;

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum BrokerErrorKind {
    #[fail(display = "Failed to connect to message broker server.")]
    ConnectionFailed,
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
