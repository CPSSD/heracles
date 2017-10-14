use errors::*;
use uuid::Uuid;
use mapreduce_job::MapReduceJob;
use queued_work_store::QueuedWork;
use std::io::{Write, BufRead, BufReader};
use std::path::PathBuf;
use std::fs;

const MEGA_BYTE: usize = 1000 * 1000;
const MAP_INPUT_SIZE: usize = MEGA_BYTE * 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MapReduceTaskStatus {
    Queued,
    InProgress,
    Complete,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskType {
    Map,
    Reduce,
}

/// The `MapReduceTask` is a struct that represents a map or reduce task.
#[derive(Clone)]
pub struct MapReduceTask {
    task_type: TaskType,
    map_reduce_id: String,
    task_id: String,

    binary_path: String,
    input_files: Vec<String>,
    output_files: Vec<String>,

    assigned_worker_id: String,
    status: MapReduceTaskStatus,
}

impl MapReduceTask {
    pub fn new(
        task_type: TaskType,
        map_reduce_id: String,
        binary_path: String,
        input_files: Vec<String>,
    ) -> Self {
        let task_id = Uuid::new_v4();
        MapReduceTask {
            task_type: task_type,
            map_reduce_id: map_reduce_id,
            task_id: task_id.to_string(),

            binary_path: binary_path,
            input_files: input_files,
            output_files: Vec::new(),

            assigned_worker_id: String::new(),
            status: MapReduceTaskStatus::Queued,
        }
    }

    pub fn get_task_type(&self) -> TaskType {
        self.task_type.clone()
    }

    pub fn get_map_reduce_id(&self) -> &str {
        &self.map_reduce_id
    }

    pub fn get_task_id(&self) -> &str {
        &self.task_id
    }

    pub fn get_binary_path(&self) -> &str {
        &self.binary_path
    }

    pub fn get_input_files(&self) -> &[String] {
        self.input_files.as_slice()
    }

    pub fn get_output_files(&self) -> &[String] {
        self.output_files.as_slice()
    }

    pub fn push_output_file(&mut self, output_file: String) {
        self.output_files.push(output_file);
    }

    pub fn get_assigned_worker_id(&self) -> &str {
        &self.assigned_worker_id
    }

    pub fn set_assigned_worker_id(&mut self, worker_id: String) {
        self.assigned_worker_id = worker_id;
    }

    pub fn get_status(&self) -> MapReduceTaskStatus {
        self.status.clone()
    }

    pub fn set_status(&mut self, new_status: MapReduceTaskStatus) {
        self.status = new_status;
    }
}

impl QueuedWork for MapReduceTask {
    type Key = String;

    fn get_work_bucket(&self) -> String {
        self.get_map_reduce_id().to_owned()
    }

    fn get_work_id(&self) -> String {
        self.get_task_id().to_owned()
    }
}

struct MapTaskFile {
    task_num: u32,
    bytes_to_write: usize,

    file: fs::File,
    file_path: String,
}

pub trait TaskProcessorTrait {
    fn create_map_tasks(&self, map_reduce_job: &MapReduceJob) -> Result<Vec<MapReduceTask>>;
}

pub struct TaskProcessor {}

impl TaskProcessor {
    fn create_new_task_file(
        &self,
        task_num: u32,
        output_directory: &PathBuf,
    ) -> Result<(MapTaskFile)> {
        let mut current_task_path: PathBuf = output_directory.clone();
        current_task_path.push(format!("map_task_{}", task_num));
        let current_task_file = fs::File::create(current_task_path.clone()).chain_err(
            || "Error creating Map input chunk file.",
        )?;

        let current_task_path_str;
        match current_task_path.to_str() {
            Some(path_str) => {
                current_task_path_str = path_str.to_owned();
            }
            None => return Err("Error getting output task path.".into()),
        }

        Ok(MapTaskFile {
            task_num: task_num,
            bytes_to_write: MAP_INPUT_SIZE,

            file: current_task_file,
            file_path: current_task_path_str,
        })
    }

    // Reads a given input file and splits it into chunks.
    fn read_input_file(
        &self,
        map_reduce_job: &MapReduceJob,
        map_task_file: &mut MapTaskFile,
        input_file: &fs::File,
        output_directory: &PathBuf,
        map_tasks: &mut Vec<MapReduceTask>,
    ) -> Result<()> {
        if map_task_file.bytes_to_write != MAP_INPUT_SIZE {
            map_task_file.file.write_all(b"\n").chain_err(
                || "Error writing line break to file",
            )?;
        }

        let buf_reader = BufReader::new(input_file);
        for line in buf_reader.lines() {
            let read_str = line.chain_err(|| "Error reading Map input.")?;
            map_task_file
                .file
                .write_all(read_str.as_bytes())
                .chain_err(|| "Error writing to Map input chunk file.")?;

            let ammount_read: usize = read_str.len();
            if ammount_read > map_task_file.bytes_to_write {
                map_tasks.push(MapReduceTask::new(
                    TaskType::Map,
                    map_reduce_job.get_map_reduce_id().to_owned(),
                    map_reduce_job.get_binary_path().to_owned(),
                    vec![map_task_file.file_path.to_owned()],
                ));

                *map_task_file =
                    self.create_new_task_file(map_task_file.task_num + 1, output_directory)
                        .chain_err(|| "Error creating Map input chunk file.")?;
            } else {
                map_task_file.bytes_to_write -= ammount_read;
            }
        }
        Ok(())
    }
}

impl TaskProcessorTrait for TaskProcessor {
    fn create_map_tasks(&self, map_reduce_job: &MapReduceJob) -> Result<Vec<MapReduceTask>> {
        let mut map_tasks = Vec::new();
        let input_directory = PathBuf::from(map_reduce_job.get_input_directory());

        let mut output_directory: PathBuf = input_directory.clone();
        output_directory.push("MapReduceTasks");
        fs::create_dir_all(output_directory.clone()).chain_err(
            || "Error creating Map tasks output directory.",
        )?;

        let mut map_task_file: MapTaskFile =
            self.create_new_task_file(1, &output_directory).chain_err(
                || "Error creating new Map input file chunk.",
            )?;

        for entry in fs::read_dir(map_reduce_job.get_input_directory())? {
            let entry: fs::DirEntry = entry.chain_err(|| "Error reading input directory.")?;
            let path: PathBuf = entry.path();
            if path.is_file() {
                let file = fs::File::open(path).chain_err(
                    || "Error opening input file.",
                )?;
                self.read_input_file(
                    map_reduce_job,
                    &mut map_task_file,
                    &file,
                    &output_directory,
                    &mut map_tasks,
                ).chain_err(|| "Error reading input file.")?;
            }
        }
        if map_task_file.bytes_to_write != MAP_INPUT_SIZE {
            map_tasks.push(MapReduceTask::new(
                TaskType::Map,
                map_reduce_job.get_map_reduce_id().to_owned(),
                map_reduce_job.get_binary_path().to_owned(),
                vec![map_task_file.file_path.to_owned()],
            ));
        }
        Ok(map_tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::io::Read;
    use std::collections::HashSet;

    // MapReduceTask Tests
    #[test]
    fn test_get_task_type() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );

        let reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );

        assert_eq!(map_task.get_task_type(), TaskType::Map);
        assert_eq!(reduce_task.get_task_type(), TaskType::Reduce);
    }

    #[test]
    fn test_get_map_reduce_id() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );
        assert_eq!(map_task.get_map_reduce_id(), "map-1");
    }

    #[test]
    fn test_get_binary_path() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );
        assert_eq!(map_task.get_binary_path(), "/tmp/bin");
    }

    #[test]
    fn test_get_input_files() {
        let map_task = MapReduceTask::new(
            TaskType::Map,
            "map-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/".to_owned()],
        );
        let input_files: &[String] = map_task.get_input_files();
        assert_eq!(input_files[0], "/tmp/input/");
    }

    #[test]
    fn test_set_output_files() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/inter_mediate".to_owned()],
        );
        reduce_task.push_output_file("output_file_1".to_owned());
        {
            let output_files: &[String] = reduce_task.get_output_files();
            assert_eq!(output_files[0], "output_file_1");
        }

        reduce_task.push_output_file("output_file_2".to_owned());
        {
            let output_files: &[String] = reduce_task.get_output_files();
            assert_eq!(output_files[0], "output_file_1");
            assert_eq!(output_files[1], "output_file_2");
        }
    }

    #[test]
    fn test_assigned_worker_id() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/inter_mediate".to_owned()],
        );
        // Assert assigned worker id starts as an empty string.
        assert_eq!(reduce_task.get_assigned_worker_id(), "");

        reduce_task.set_assigned_worker_id("worker-1".to_owned());
        assert_eq!(reduce_task.get_assigned_worker_id(), "worker-1");
    }

    #[test]
    fn test_set_status() {
        let mut reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/inter_mediate".to_owned()],
        );
        // Assert that the default status for a task is Queued.
        assert_eq!(reduce_task.get_status(), MapReduceTaskStatus::Queued);

        // Set the status to Completed and assert success.
        reduce_task.set_status(MapReduceTaskStatus::Complete);
        assert_eq!(reduce_task.get_status(), MapReduceTaskStatus::Complete);
    }

    #[test]
    fn test_queued_work_impl() {
        let reduce_task = MapReduceTask::new(
            TaskType::Reduce,
            "reduce-1".to_owned(),
            "/tmp/bin".to_owned(),
            vec!["/tmp/input/inter_mediate".to_owned()],
        );

        assert_eq!(reduce_task.get_work_bucket(), "reduce-1");
        assert_eq!(reduce_task.get_work_id(), reduce_task.get_task_id());
    }

    #[test]
    fn test_create_map_tasks() {
        let task_processor = TaskProcessor {};

        let test_path = Path::new("/tmp/cerberus/create_task_test/").to_path_buf();
        let mut input_path1 = test_path.clone();
        input_path1.push("input-1");
        let mut input_path2 = test_path.clone();
        input_path2.push("input-2");

        fs::create_dir_all(test_path.clone()).unwrap();
        let mut input_file1 = fs::File::create(input_path1.clone()).unwrap();
        input_file1
            .write_all(b"this is the first test file")
            .unwrap();
        let mut input_file2 = fs::File::create(input_path2.clone()).unwrap();
        input_file2
            .write_all(b"this is the second test file")
            .unwrap();

        let map_reduce_job = MapReduceJob::new(
            "test-client".to_owned(),
            "/tmp/bin".to_owned(),
            test_path.to_str().unwrap().to_owned(),
        );

        let map_tasks: Vec<MapReduceTask> =
            task_processor.create_map_tasks(&map_reduce_job).unwrap();

        assert_eq!(map_tasks.len(), 1);
        assert_eq!(map_tasks[0].get_task_type(), TaskType::Map);
        assert_eq!(
            map_tasks[0].get_map_reduce_id(),
            map_reduce_job.get_map_reduce_id()
        );
        assert_eq!(
            map_tasks[0].get_binary_path(),
            map_reduce_job.get_binary_path()
        );
        assert_eq!(map_tasks[0].input_files.len(), 1);

        // Read map task input and make sure it is as expected.
        let mut input_file = fs::File::open(map_tasks[0].input_files[0].clone()).unwrap();
        let mut map_input = String::new();
        input_file.read_to_string(&mut map_input).unwrap();

        // Either input file order is fine.
        let mut good_inputs = HashSet::new();
        good_inputs.insert(
            "this is the first test file\nthis is the second test file".to_owned(),
        );
        good_inputs.insert(
            "this is the second test file\nthis is the first test file".to_owned(),
        );

        assert!(good_inputs.contains(&map_input));
    }
}
