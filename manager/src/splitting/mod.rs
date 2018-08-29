pub mod map;
pub mod reduce;

use std::fmt;
use std::fmt::Display;

use failure::*;

// use heracles_proto::datatypes::{Job, Task};

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
    #[fail(display = "Failed to access the output file directory.")]
    OutputDirectoryOpenFailed,
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
