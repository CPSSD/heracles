//! Module for taking a `Job` and splitting it into a set of `Task`s.

mod text;

use std::fmt::Display;
use std::fmt;

use failure::*;
use cerberus_proto::datatypes::*;

pub fn split_job(job: &Job) -> Result<Vec<Task>, Error> {
    match job.get_input_kind() {
        InputDataKind::UNDEFINED => Err(SplitterErrorKind::InvalidInputDataKind.into()),
        InputDataKind::DATA_TEXT_NEWLINES => text::LineSplitter::split(job),
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum SplitterErrorKind {
    #[fail(display = "Failed to open input file for processing.")]
    FileOpenFailed,
    #[fail(display = "Failed to read input file.")]
    FileReadFailed,
    #[fail(display = "An unknown I/O error has occurred.")]
    GenericIOError,
    #[fail(display = "Failed to access the input file directory.")]
    InputDirectoryOpenFailed,
    #[fail(display = "Cannot have UNDEFINED InputDataKind.")]
    InvalidInputDataKind,
}

#[derive(Debug)]
pub struct SplitterError {
    inner: Context<SplitterErrorKind>,
}

impl SplitterError {
    pub fn kind(&self) -> SplitterErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for SplitterError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for SplitterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl From<SplitterErrorKind> for SplitterError {
    fn from(kind: SplitterErrorKind) -> SplitterError {
        SplitterError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<SplitterErrorKind>> for SplitterError {
    fn from(inner: Context<SplitterErrorKind>) -> SplitterError {
        SplitterError { inner }
    }
}
