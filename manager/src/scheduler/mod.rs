use std::fmt;
use std::fmt::Display;

use failure::*;
use futures::sync::mpsc;

use heracles_proto::datatypes::*;

pub struct Scheduler {
    broker_handle: mpsc::Sender<Task>,
}

impl Scheduler {
    pub fn new(handle: mpsc::Sender<Task>) -> Self {
        Scheduler {
            broker_handle: handle,
        }
    }

    pub fn schedule(&self, _job: &Job) -> Result<String, SchedulerError> {
        unimplemented!()
    }

    pub fn cancel(&self, _job_id: &str) -> Result<(), SchedulerError> {
        unimplemented!()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum SchedulerErrorKind {
    #[fail(display = "Undefined")]
    Undefined,
}

#[derive(Debug)]
pub struct SchedulerError {
    inner: Context<SchedulerErrorKind>,
}

impl SchedulerError {
    pub fn kind(&self) -> SchedulerErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for SchedulerError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl From<SchedulerErrorKind> for SchedulerError {
    fn from(kind: SchedulerErrorKind) -> SchedulerError {
        SchedulerError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<SchedulerErrorKind>> for SchedulerError {
    fn from(inner: Context<SchedulerErrorKind>) -> SchedulerError {
        SchedulerError { inner }
    }
}
