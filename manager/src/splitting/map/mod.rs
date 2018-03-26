//! Module for taking a `Job` and splitting it into a set of Map `Task`s.

mod text;

use failure::*;

use super::SplitterErrorKind;
use heracles_proto::datatypes::*;

pub fn split(job: &Job) -> Result<Vec<Task>, Error> {
    match job.get_input_kind() {
        InputDataKind::UNDEFINED => Err(SplitterErrorKind::InvalidInputDataKind.into()),
        InputDataKind::DATA_TEXT_NEWLINES => text::LineSplitter::split(job),
    }
}
