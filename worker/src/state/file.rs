use std::fs;
use std::io::Write;
use std::path::PathBuf;

use protobuf::Message;

use heracles_proto::datatypes::{Task, TaskKind, TaskStatus};
use super::*;

// TODO: Make sure they are consistent or merge them with the master side of the library.
const JOBS_DIR: &str = "jobs";
const TASKS_DIR: &str = "tasks";
const PENDING_MAP_DIR: &str = "pending_map_tasks";
const PENDING_REDUCE_DIR: &str = "pending_reduce_tasks";

pub struct FileStore {
    path: PathBuf,
}

impl FileStore {
    pub fn new(path: &PathBuf) -> Result<Self, StateError> {
        Ok(FileStore { path: path.clone() })
    }

    /// Creates the path to the job from the given `job_id`.
    fn job_dir_path(&self, job_id: &str) -> PathBuf {
        self.path.join(JOBS_DIR).join(job_id)
    }
}

impl State for FileStore {
    fn save_progress(&self, task: &Task) -> Result<(), StateError> {
        let job_dir_path = self.job_dir_path(task.get_job_id());
        let task_id = task.get_id();

        let task_file_path = job_dir_path.join(TASKS_DIR).join(&task_id);
        if !task_file_path.exists() {
            return Err(StateErrorKind::MissingTask.into());
        }

        let mut pending_file_path = job_dir_path.clone();
        match task.get_kind() {
            TaskKind::MAP => pending_file_path.push(PENDING_MAP_DIR),
            TaskKind::REDUCE => pending_file_path.push(PENDING_REDUCE_DIR),
        }
        pending_file_path.push(&task_id);
        if !pending_file_path.exists() {
            return Err(StateErrorKind::MissingPendingTask.into());
        }

        let serialized = task.write_to_bytes()
            .context(StateErrorKind::TaskSerialisationFailed)?;
        fs::OpenOptions::new()
            .write(true)
            .open(task_file_path)
            .context(StateErrorKind::TaskWriteFailed)?
            .write_all(&serialized)
            .context(StateErrorKind::TaskWriteFailed)?;

        // Remove the pending task if its done.
        if task.get_status() == TaskStatus::TASK_DONE {
            fs::remove_file(pending_file_path).context(StateErrorKind::RemovingPendingTaskFailed)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs;
    use std::fs::File;
    use super::*;
    use super::State;

    fn setup(test_path: &PathBuf, task_id: &str) {
        fs::create_dir_all(&test_path).unwrap();

        let test_tasks_path = test_path.join(TASKS_DIR);
        fs::create_dir_all(&test_tasks_path).unwrap();
        // File content doesn't matter, we don't really care about it.
        File::create(test_tasks_path.join(&task_id))
            .unwrap()
            .write_all(task_id.as_bytes())
            .unwrap();

        let test_pending_tasks_path = test_path.join(PENDING_MAP_DIR);
        fs::create_dir_all(&test_pending_tasks_path).unwrap();
        // File content doesn't matter
        File::create(test_pending_tasks_path.join(&task_id))
            .unwrap()
            .write_all(task_id.clone().as_bytes())
            .unwrap();
    }

    fn check_map_exists(test_path: &PathBuf, task_id: &str) -> bool {
        test_path.join(PENDING_MAP_DIR).join(task_id).exists()
    }

    fn cleanup(test_dir: &PathBuf) {
        fs::remove_dir_all(test_dir).unwrap();
    }

    #[test]
    fn test_file_store() {
        let test_dir = temp_dir().join("heracles_test").join("state");

        let st = FileStore::new(&test_dir).unwrap();

        let task_id = "test_task";
        let job_id = "test_job";

        let test_job_path = st.job_dir_path(job_id.clone());
        assert_eq!(
            "/tmp/heracles_test/state/jobs/test_job",
            test_job_path.to_str().unwrap()
        );
        setup(&test_job_path, task_id.clone());

        let mut test_task = Task::new();
        test_task.set_id(task_id.clone().into());
        test_task.set_job_id(job_id.clone().into());
        test_task.set_kind(TaskKind::MAP);
        test_task.set_status(TaskStatus::TASK_IN_PROGRESS);
        st.save_progress(&test_task).unwrap();

        assert_eq!(true, check_map_exists(&test_job_path, task_id.clone()));

        test_task.set_status(TaskStatus::TASK_DONE);
        st.save_progress(&test_task).unwrap();

        assert_eq!(false, check_map_exists(&test_job_path, task_id.clone()));

        cleanup(&test_dir);
    }
}
