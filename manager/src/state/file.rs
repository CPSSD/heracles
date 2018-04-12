use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use protobuf;
use protobuf::Message;

use super::*;

const JOB_DIR: &str = "jobs";
const SCHEDULERS_DIR: &str = "schedulers";
const JOB_SAVE_FILE: &str = "request";
const TASKS_DIR: &str = "tasks";
const PENDING_MAP_DIR: &str = "pending_map_tasks";
const PENDING_REDUCE_DIR: &str = "pending_reduce_tasks";

/// Save the state data in filesystem
pub struct FileStore {
    path: PathBuf,
}

impl FileStore {
    // Creates a new file backed state storing
    pub fn new(path: &PathBuf) -> Result<Self, StateError> {
        fs::create_dir_all(path.join(JOB_DIR)).context(StateErrorKind::JobsFolderCreationFailed)?;
        Ok(FileStore { path: path.clone() })
    }

    /// Prepares a job directory for a given job. If a directory already exists, or if everything
    /// went ok, [`PathBuf`] is returned with the location of the job folder.
    /// [`std::result::Ok`] is returned, otherwise if an error occured while creating the
    /// directory, [`StateError`] is raised;
    fn prepare_job_directory(&self, job_dir_path: &PathBuf) -> Result<(), StateError> {
        if job_dir_path.exists() {
            return Ok(());
        }
        fs::create_dir_all(job_dir_path.join(TASKS_DIR))
            .context(StateErrorKind::TasksFolderCreationFailed)?;
        fs::create_dir_all(job_dir_path.join(PENDING_MAP_DIR))
            .context(StateErrorKind::MapTasksFolderCreationFailed)?;
        fs::create_dir_all(job_dir_path.join(PENDING_REDUCE_DIR))
            .context(StateErrorKind::ReduceTasksFolderCreationFailed)?;
        Ok(())
    }

    /// Creates the path to the job from the given `job_id`.
    fn job_dir_path(&self, job_id: &str) -> PathBuf {
        self.path.join(JOB_DIR).join(job_id)
    }

    // TODO: Add to [`State`] trait.
    /// Removes a job and all of its contents from the list of jobs.
    fn remove_job(&self, job_id: &str) -> Result<(), StateError> {
        Ok(fs::remove_dir_all(self.job_dir_path(job_id))
            .context(StateErrorKind::JobsFolderRemoveFailed)?)
    }

    /// Lists pending tasks for a specified type of task in a job.
    fn list_pending_tasks(&self, job_id: &str, kind: TaskKind) -> Result<Vec<String>, StateError> {
        let job_dir_path = self.job_dir_path(job_id);

        let mut pending_dir_path = job_dir_path.clone();
        match kind {
            TaskKind::MAP => pending_dir_path.push(PENDING_MAP_DIR),
            TaskKind::REDUCE => pending_dir_path.push(PENDING_REDUCE_DIR),
        }

        fs::read_dir(&pending_dir_path)
            .context(StateErrorKind::PendingTasksListFailed)?
            .map(|entry| {
                // We can ignore the last unwrap as its only a precaution if the file name is not
                // a string for some random OS.
                Ok(entry
                    .context(StateErrorKind::GenericIOError)?
                    .file_name()
                    .into_string()
                    .unwrap())
            })
            .collect::<Result<Vec<String>, StateError>>()
    }

    /// Gets the full task details of map or reduce tasks which have not yet completed.
    fn pending_tasks_data(&self, job_id: &str, kind: TaskKind) -> Result<Vec<Task>, StateError> {
        let tasks_dir_path = self.job_dir_path(job_id).join(TASKS_DIR);

        self.list_pending_tasks(job_id, kind)?
            .iter()
            .map(|task_id| {
                let mut f = File::open(tasks_dir_path.join(task_id))
                    .context(StateErrorKind::TaskFileOpenFailed)?;
                Ok(protobuf::core::parse_from_reader::<Task>(&mut f)
                    .context(StateErrorKind::TaskDeserialisationFailed)?)
            })
            .collect::<Result<Vec<Task>, StateError>>()
    }
}

impl State for FileStore {
    fn save_job(&self, job: &Job) -> Result<(), StateError> {
        let job_dir_path = self.job_dir_path(job.get_id());
        if let Err(err) = self.prepare_job_directory(&job_dir_path) {
            self.remove_job(job.get_id())?;
            return Err(err);
        }

        let serialized = job.write_to_bytes()
            .context(StateErrorKind::JobSerialisationFailed)?;
        Ok(File::create(&job_dir_path.join(JOB_SAVE_FILE))
            .context(StateErrorKind::JobWriteFailed)?
            .write_all(&serialized)
            .context(StateErrorKind::JobWriteFailed)?)
    }

    fn save_task(&self, task: &Task) -> Result<(), StateError> {
        let job_dir_path = self.job_dir_path(task.get_job_id());
        let task_id = task.get_id();

        let serialized = task.write_to_bytes()
            .context(StateErrorKind::TaskSerialisationFailed)?;
        File::create(&job_dir_path.join(TASKS_DIR).join(task_id))
            .context(StateErrorKind::TaskWriteFailed)?
            .write_all(&serialized)
            .context(StateErrorKind::TaskWriteFailed)?;

        // Save to either map or reduce pending tasks
        let mut pending_file_path = job_dir_path.clone();
        match task.get_kind() {
            TaskKind::MAP => pending_file_path.push(PENDING_MAP_DIR),
            TaskKind::REDUCE => pending_file_path.push(PENDING_REDUCE_DIR),
        }
        pending_file_path.push(task_id);

        match task.get_status() {
            TaskStatus::TASK_UNKNOWN => Ok(()),
            TaskStatus::TASK_IN_PROGRESS => Ok(()),
            TaskStatus::TASK_PENDING => {
                File::create(pending_file_path)
                    .context(StateErrorKind::PendingTaskWriteFailed)?
                    .write_all(&task_id.as_bytes())
                    .context(StateErrorKind::PendingTaskWriteFailed)?;
                Ok(())
            }
            TaskStatus::TASK_DONE => {
                fs::remove_file(pending_file_path)
                    .context(StateErrorKind::PendingTaskRemoveFailed)?;
                Ok(())
            }
            TaskStatus::TASK_FAILED => {
                // TODO: Do we want to remove the pending file if the task
                //       fails or do we keep it?
                Ok(())
            }
        }
    }

    fn pending_map_tasks(&self, job: &Job) -> Result<Vec<Task>, StateError> {
        self.pending_tasks_data(job.get_id().into(), TaskKind::MAP)
    }

    fn pending_reduce_tasks(&self, job: &Job) -> Result<Vec<Task>, StateError> {
        self.pending_tasks_data(job.get_id().into(), TaskKind::REDUCE)
    }

    fn map_done(&self, job: &Job) -> Box<Future<Item = (), Error = StateError>> {
        unimplemented!()
    }

    fn reduce_done(&self, job: &Job) -> Box<Future<Item = (), Error = StateError>> {
        unimplemented!()
    }
}
