//! Module for splitting text-based input files at various delimiters.

use std::fs::{read_dir, DirEntry, File};
use std::io;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::Utc;
use failure::*;
use rayon::prelude::*;
use uuid::Uuid;

use super::*;
use heracles_proto::datatypes::*;
use settings::SETTINGS;

pub struct LineSplitter;

impl LineSplitter {
    /// Splits a job into a set of tasks.
    pub fn split(job: &Job) -> Result<Vec<Task>, Error> {
        let mut ret = Vec::new();
        let entries: Vec<io::Result<DirEntry>> = read_dir(job.get_input_directory())
            .context(SplitterErrorKind::InputDirectoryOpenFailed)?
            .collect();
        // Maps each directory entry to a vector of input chunks, then flattens them all into a
        // single vector of chunks.
        let input_chunks: Vec<InputChunk> = entries
            .into_par_iter()
            .map(|entry| {
                let path = entry.context(SplitterErrorKind::GenericIOError)?.path();
                LineSplitter::split_file(path)
            })
            .collect::<Result<Vec<Vec<InputChunk>>, Error>>()?
            .into_iter()
            .flat_map(|v| v)
            .collect();
        for input in input_chunks {
            let mut task = Task::new();
            task.set_id(Uuid::new_v4().to_string());
            task.set_job_id(job.get_id().to_string());
            task.set_status(TaskStatus::TASK_PENDING);
            task.set_kind(TaskKind::MAP);
            task.set_time_created(Utc::now().timestamp() as u64);
            task.set_input_chunk(input);
            task.set_payload_path(job.get_payload_path().to_string());
            ret.push(task);
        }
        Ok(ret)
    }

    /// Splits a single input file into a set of chunks. Each map task gets one chunk.
    fn split_file<P: AsRef<Path> + Clone>(p: P) -> Result<Vec<InputChunk>, Error> {
        let mut ret = Vec::new();
        let f = File::open(p.clone()).context(SplitterErrorKind::FileOpenFailed)?;
        let reader = BufReader::new(f);
        let mut amount_read_this_chunk: u64 = 0;
        let mut chunk_start_index: u64 = 0;
        let task_input_size: u64 = SETTINGS.read().unwrap().get("scheduler.input_chunk_size")?;

        for line in reader.lines() {
            let line = line.context(SplitterErrorKind::FileReadFailed)?;
            // Currently this means that the function only supports text files with UNIX-style line
            // endings. Although the chunking does not need to be so accurate that a single
            // character will make much of a difference.
            let len_with_newline_char = line.len() + 1;
            if amount_read_this_chunk + len_with_newline_char as u64 > task_input_size {
                // Check for the special case where the current chunk is empty and the next line
                // would put it over capacity. In that case we have no choice but to take the line
                // whole.
                if amount_read_this_chunk == 0 {
                    amount_read_this_chunk += len_with_newline_char as u64;
                }
                ret.push(LineSplitter::create_input_file(
                    p.clone(),
                    chunk_start_index,
                    amount_read_this_chunk,
                ));
                amount_read_this_chunk = 0;
                chunk_start_index += amount_read_this_chunk;
            }
            amount_read_this_chunk += len_with_newline_char as u64;
        }
        // Add the final chunk
        ret.push(LineSplitter::create_input_file(
            p,
            chunk_start_index,
            amount_read_this_chunk,
        ));

        Ok(ret)
    }

    /// Small helper function to create an `InputChunk` proto.
    fn create_input_file<P: AsRef<Path>>(path: P, start: u64, end: u64) -> InputChunk {
        let mut ret = InputChunk::new();
        ret.set_path(path.as_ref().to_string_lossy().to_string());
        ret.set_start_byte(start);
        ret.set_end_byte(end);
        ret
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_splitting_small_file() {
        let mut dir: PathBuf = env!("CARGO_MANIFEST_DIR").into();
        dir.push("testdata");
        dir.push("text_with_newlines");
        SETTINGS
            .write()
            .unwrap()
            .set("task_input_size", 1024)
            .unwrap();
        let mut test_job = Job::new();
        test_job.set_input_directory(dir.to_str().unwrap().to_string());

        let tasks = LineSplitter::split(&test_job).unwrap();

        assert_eq!(2, tasks.len());
        assert_eq!(1003, tasks[0].get_input_chunk().get_end_byte());
        assert_eq!(838, tasks[1].get_input_chunk().get_end_byte());
    }
}
