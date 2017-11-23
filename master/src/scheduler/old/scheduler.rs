use chrono::prelude::*;
use serde_json;

use errors::*;
use common::{Job, Task, TaskStatus};
use mapreduce_tasks::TaskProcessorTrait;
use queued_work_store::{QueuedWork, QueuedWorkStore};
use state;

use cerberus_proto::mapreduce::Status as JobStatus;
use cerberus_proto::worker as pb;

const TASK_FAILURE_THRESHOLD: u16 = 10;

/// `Scheduler` holds the state related to scheduling and processes `Job`s.
/// It does not schedule `Task`s on workers, but instead maintains a queue that is
/// processed by the `scheduling_loop`.
///
/// Only one `Job` is `IN_PROGRESS` at a time.
pub struct Scheduler {
    // job_queue stores `Job`s submitted by clients. Even when jobs are not
    // currently queued job_queue still maintains ownership of the job.
    job_queue: QueuedWorkStore<Job>,

    // task_queue stores `Task`s for the currently in progress `Job`.
    // Even when tasks are not currently queued task_queue still maintains ownership of
    // the task.
    task_queue: QueuedWorkStore<Task>,

    map_reduce_in_progress: bool,
    in_progress_job_id: Option<String>,

    available_workers: u32,
    task_processor: Box<TaskProcessorTrait + Send>,
}

impl Scheduler {
    pub fn new(task_processor: Box<TaskProcessorTrait + Send>) -> Self {
        let job_queue: QueuedWorkStore<Job> = QueuedWorkStore::new();
        let task_queue: QueuedWorkStore<Task> = QueuedWorkStore::new();
        Scheduler {
            job_queue: job_queue,
            task_queue: task_queue,

            map_reduce_in_progress: false,
            in_progress_job_id: None,

            available_workers: 0,
            task_processor: task_processor,
        }
    }

    /// `process_next_map_reduce` is used to make the next `Job` in the queue the active
    /// job being processed when the current job is completed.
    ///
    /// It creates the Map tasks for the `Job` and adds them to the queue so that they can
    /// be assigned to workers.
    fn process_next_map_reduce(&mut self) -> Result<()> {
        match self.job_queue.pop_queue_top() {
            Some(job) => {
                info!("Starting Map Reduce Job ({})", job.id);

                job.status = JobStatus::IN_PROGRESS;
                job.time_started = Some(Utc::now());
                self.map_reduce_in_progress = true;
                self.in_progress_job_id = Some(job.id.clone());

                let map_tasks: Vec<Task> = self.task_processor.create_map_tasks(job)?;
                job.map_tasks_total = map_tasks.len() as u32;

                for task in map_tasks {
                    self.task_queue.add_to_store(Box::new(task)).chain_err(
                        || "Error adding map reduce task to queue",
                    )?;
                }
                Ok(())
            }
            None => Err("no queued map reduce".into()),
        }
    }

    //TODO(conor): Remove this when get_map_reduce_in_progress is used.
    #[allow(dead_code)]
    pub fn get_map_reduce_in_progress(&self) -> bool {
        self.map_reduce_in_progress
    }

    //TODO(conor): Remove this when get_in_progress_job_id is used.
    #[allow(dead_code)]
    pub fn get_in_progress_job_id(&self) -> Option<String> {
        self.in_progress_job_id.clone()
    }

    pub fn get_job_queue_size(&self) -> usize {
        if self.map_reduce_in_progress {
            return self.job_queue.queue_size() + (1 as usize);
        }
        self.job_queue.queue_size()
    }

    pub fn get_task_queue_size(&self) -> usize {
        self.task_queue.queue_size()
    }

    /// `schedule_job` is used to add a `Job` to the queue. If there is no
    /// current in progress job, this job will be made active.
    pub fn schedule_job(&mut self, job: Job) -> Result<()> {
        info!(
            "Adding Map Reduce Job ({}) to queue. input={} output={}",
            job.id,
            job.input_directory,
            job.output_directory
        );

        self.job_queue.add_to_store(Box::new(job)).chain_err(
            || "Error adding map reduce job to queue.",
        )?;
        if !self.map_reduce_in_progress {
            self.process_next_map_reduce().chain_err(
                || "Error processing next map reduce.",
            )?;
        }
        Ok(())
    }

    pub fn get_available_workers(&self) -> u32 {
        self.available_workers
    }

    pub fn set_available_workers(&mut self, available_workers: u32) {
        self.available_workers = available_workers;
    }

    pub fn get_mapreduce_status(&self, mapreduce_id: &str) -> Result<&Job> {
        let result = self.job_queue.get_work_by_id(&mapreduce_id.to_owned());
        match result {
            None => Err("There was an error getting the result".into()),
            Some(job) => Ok(job),
        }
    }

    /// `get_mapreduce_client_status` returns a vector of `Job`s for a given client.
    pub fn get_mapreduce_client_status(&self, client_id: &str) -> Result<Vec<&Job>> {
        if self.job_queue.has_work_bucket(&client_id.to_owned()) {
            return self.job_queue.get_work_bucket_items(&client_id.to_owned());
        }
        Ok(Vec::new())
    }

    pub fn pop_queued_task(&mut self) -> Option<&mut Task> {
        self.task_queue.pop_queue_top()
    }

    /// `unschedule_task` moves a task that was previously assigned to a worker back into the queue
    /// to be reassigned.
    pub fn unschedule_task(&mut self, task_id: &str) -> Result<()> {
        self.task_queue
            .move_task_to_queue(task_id.to_owned())
            .chain_err(|| "Error unscheduling map reduce task")?;

        let task = self.task_queue
            .get_work_by_id_mut(&task_id.to_owned())
            .chain_err(|| "Error unschuling map reduce task")?;

        task.assigned_worker_id = String::new();
        task.status = TaskStatus::Queued;

        Ok(())
    }

    fn create_reduce_tasks(&mut self, job_id: &str) -> Result<()> {
        info!(
            "Completed Map Tasks for job ({}), creating reduce tasks.",
            job_id
        );

        let reduce_tasks_total = {
            let job = self.job_queue
                .get_work_by_id_mut(&job_id.to_owned())
                .chain_err(|| "Error creating reduce tasks.")?;

            let reduce_tasks = {
                let map_tasks = self.task_queue
                    .get_work_bucket_items(&job_id.to_owned())
                    .chain_err(|| "Error creating reduce tasks.")?;

                self.task_processor
                    .create_reduce_tasks(job, map_tasks.as_slice())
                    .chain_err(|| "Error creating reduce tasks.")?
            };

            job.reduce_tasks_total = reduce_tasks.len() as u32;

            for reduce_task in reduce_tasks {
                self.task_queue
                    .add_to_store(Box::new(reduce_task))
                    .chain_err(|| "Error adding reduce task to store.")?;
            }

            job.reduce_tasks_total
        };

        if reduce_tasks_total == 0 {
            self.complete_current_job().chain_err(
                || "Error completing map reduce job.",
            )?;
        }

        Ok(())
    }

    /// `increment_map_tasks_completed` increments the number of completed map tasks for a given
    /// job.
    /// If all the map tasks have been completed it will create and add reduce tasks to the queue.
    fn increment_map_tasks_completed(&mut self, job_id: &str, cpu_time: u64) -> Result<()> {
        let all_maps_completed: bool = {
            let job = self.job_queue
                .get_work_by_id_mut(&job_id.to_owned())
                .chain_err(|| "Error incrementing completed map tasks.")?;

            job.map_tasks_completed += 1;
            job.cpu_time += cpu_time;
            job.map_tasks_completed == job.map_tasks_total
        };

        if all_maps_completed {
            // Create Reduce tasks.
            self.create_reduce_tasks(job_id).chain_err(
                || "Error incrementing completed map tasks.",
            )?;
        }
        Ok(())
    }

    /// `process_map_task_response` processes the map task response returned by a worker.
    /// If the task failed it will be moved back into the queue to be assigned to another worker.
    pub fn process_map_task_response(
        &mut self,
        map_task_id: &str,
        map_response: &pb::MapResult,
    ) -> Result<()> {
        if map_response.status == pb::ResultStatus::SUCCESS {
            let job_id = {
                let map_task = self.task_queue
                    .get_work_by_id_mut(&map_task_id.to_owned())
                    .chain_err(|| "Error marking map task as completed.")?;

                // TODO(conor): When we add retrying for map tasks after reduce tasks have already
                // been completed, add some logic here to update the input of those reduce jobs.
                if map_task.status == TaskStatus::Complete {
                    return Ok(());
                }

                map_task.status = TaskStatus::Complete;
                for (partition, output_file) in map_response.get_map_results() {
                    map_task.map_output_files.insert(
                        *partition,
                        output_file.to_owned(),
                    );
                }
                map_task.job_id.to_owned()
            };
            self.increment_map_tasks_completed(&job_id, map_response.get_cpu_time())
                .chain_err(|| "Error marking map task as completed.")?;
        } else {
            self.unschedule_task(map_task_id).chain_err(
                || "Error marking map task as complete.",
            )?;
            self.handle_task_failure(
                map_task_id,
                map_response.get_failure_details(),
            )?;
        }
        Ok(())
    }

    fn complete_current_job(&mut self) -> Result<()> {
        let job_id = self.get_in_progress_job_id().chain_err(
            || "Unable to get ID of in-progress job.",
        )?;

        {
            let job = self.job_queue.get_work_by_id_mut(&job_id).chain_err(
                || "Mapreduce job not found in queue.",
            )?;

            self.task_queue
                .remove_work_bucket(&job.get_work_id())
                .chain_err(|| "Error marking map reduce job as complete.")?;

            job.status = JobStatus::DONE;
            job.time_completed = Some(Utc::now());

            info!("Completed Map Reduce Job ({}).", job_id);
            info!("Total CPU time used: {}", job.cpu_time);
        }

        self.map_reduce_in_progress = false;
        self.in_progress_job_id = None;

        if self.job_queue.queue_size() > 0 {
            self.process_next_map_reduce().chain_err(
                || "Error incrementing completed reduce tasks.",
            )?;
        }

        Ok(())
    }

    /// `increment_reduce_tasks_completed` increments the completed reduce tasks for a given job.
    /// If all the reduce tasks have been completed it will make the next `Job` in the
    /// queue active, if one exists.
    fn increment_reduce_tasks_completed(&mut self, job_id: &str, cpu_time: u64) -> Result<()> {
        let completed_map_reduce: bool = {
            let job = self.job_queue
                .get_work_by_id_mut(&job_id.to_owned())
                .chain_err(|| "Mapreduce job not found in queue.")?;

            job.reduce_tasks_completed += 1;
            job.cpu_time += cpu_time;

            job.reduce_tasks_completed == job.reduce_tasks_total
        };

        if completed_map_reduce {
            self.complete_current_job().chain_err(
                || "Error completing current job.",
            )?;
        }
        Ok(())
    }

    /// `process_reduce_task_response` processes the reduce task response returned by a worker.
    /// If the task failed it will be moved back into the queue to be assigned to another worker.
    pub fn process_reduce_task_response(
        &mut self,
        reduce_task_id: &str,
        reduce_response: &pb::ReduceResult,
    ) -> Result<()> {

        if reduce_response.get_status() == pb::ResultStatus::SUCCESS {
            let job_id = {
                let reduce_task = self.task_queue
                    .get_work_by_id_mut(&reduce_task_id.to_owned())
                    .chain_err(|| "Error marking reduce task as completed.")?;

                // If a result for a reduce task has already been returned, make sure we don't
                // increment completed reduce tasks again.
                if reduce_task.status == TaskStatus::Complete {
                    return Ok(());
                }

                reduce_task.status = TaskStatus::Complete;
                reduce_task.job_id.to_owned()
            };

            self.increment_reduce_tasks_completed(&job_id, reduce_response.get_cpu_time())
                .chain_err(|| "Error marking reduce task as completed.")?;
        } else {
            self.unschedule_task(reduce_task_id).chain_err(
                || "Error marking reduce task as complete.",
            )?;
            self.handle_task_failure(
                reduce_task_id,
                reduce_response.get_failure_details(),
            )?;
        }
        Ok(())
    }

    fn task_exceeds_failure_threshold(&self, task: &Task) -> bool {
        task.failure_count >= TASK_FAILURE_THRESHOLD
    }

    fn fail_current_job(&mut self, failure_reason: String) -> Result<()> {
        let job_id = self.get_in_progress_job_id().chain_err(
            || "Unable to get ID of in-progress job.",
        )?;
        {
            let job = self.job_queue
                .get_work_by_id_mut(&job_id.to_owned())
                .chain_err(|| format!("Unable to get job with ID {}.", job_id))?;
            self.task_queue
                .remove_work_bucket(&job.get_work_id())
                .chain_err(|| "Error removing failed job from the queue.")?;
            job.status = JobStatus::FAILED;
            job.status_details = Some(failure_reason);
        }

        self.map_reduce_in_progress = false;
        self.in_progress_job_id = None;

        if !self.job_queue.queue_empty() {
            self.process_next_map_reduce().chain_err(
                || "Error scheduling next map reduce.",
            )?;
        }
        Ok(())
    }

    fn handle_task_failure(&mut self, task_id: &str, failure_details: &str) -> Result<()> {
        {
            let task_mut = self.task_queue
                .get_work_by_id_mut(&task_id.to_owned())
                .chain_err(|| "Error fetching map task.")?;

            task_mut.failure_count += 1;
            if !failure_details.is_empty() {
                task_mut.failure_details = Some(failure_details.to_owned());
            }
        }

        let task = self.task_queue
            .get_work_by_id(&task_id.to_owned())
            .chain_err(|| "Error fetching map task.")?
            .clone();
        if self.task_exceeds_failure_threshold(&task) {
            warn!(
                "Task with ID {} has failed over {} times. Marking current job as failed.",
                task_id,
                TASK_FAILURE_THRESHOLD
            );

            let failure_reason = match task.failure_details {
                Some(details) => details,
                None => "Task belonging to this job failed too many times.".to_owned(),
            };

            self.fail_current_job(failure_reason).chain_err(
                || "Error marking current job as failed.",
            )?;
        }
        Ok(())
    }
}

impl state::StateHandling for Scheduler {
    fn new_from_json(_: serde_json::Value) -> Result<Self> {
        Err("Unable to create Scheduler from JSON.".into())
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        // Create a JSON list from the QueuedWorkStore for MapReduce Jobs.
        let mut jobs_json: Vec<serde_json::Value> = Vec::new();
        let mut queued_job_ids: Vec<serde_json::Value> = Vec::new();
        let jobs = self.job_queue.get_all_store_items().chain_err(
            || "Unable to retrieve job_queue store items",
        )?;
        for job in jobs {
            let job_id = job.get_work_id();
            if self.job_queue.task_in_queue(&job_id) {
                queued_job_ids.push(json!(job_id));
            }
            jobs_json.push(job.dump_state().chain_err(|| "Unable to dump Job state")?);
        }

        // Create a JSON list from the QueuedWorkStore for MapReduce Task.
        let mut map_reduce_tasks_json: Vec<serde_json::Value> = Vec::new();
        let mut queued_map_reduce_task_ids: Vec<serde_json::Value> = Vec::new();
        let map_reduce_tasks = self.task_queue.get_all_store_items().chain_err(
            || "Unable to retrieve all store items from task_queue",
        )?;
        for map_reduce_task in map_reduce_tasks {
            let map_reduce_task_id = map_reduce_task.get_work_id();
            if self.task_queue.task_in_queue(&map_reduce_task_id) {
                queued_map_reduce_task_ids.push(json!(map_reduce_task_id));
            }
            map_reduce_tasks_json.push(map_reduce_task.dump_state().chain_err(
                || "Unable to dump Task state",
            )?);
        }

        // Generate the JSON representation of this objects state.
        Ok(json!({
            "job_store_size": self.job_queue.store_size(),
            "job_store": json!(jobs_json),
            "queued_job_ids": json!(queued_job_ids),

            "map_reduce_task_store_size": self.task_queue.store_size(),
            "map_reduce_task_store": json!(map_reduce_tasks_json),
            "queued_map_reduce_task_ids": json!(queued_map_reduce_task_ids),

            "map_reduce_in_progress": self.map_reduce_in_progress,
            "in_progress_job_id": self.in_progress_job_id,

            "available_workers": self.available_workers,
        }))

    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {

        // Handle map reduce job queue.
        let job_store_size: usize = serde_json::from_value(data["job_store_size"].clone())
            .chain_err(|| "Unable to convert job_store_size")?;

        self.map_reduce_in_progress = serde_json::from_value(
            data["map_reduce_in_progress"].clone(),
        ).chain_err(|| "Unable to convert map_reduce_in_progress")?;

        self.in_progress_job_id = serde_json::from_value(data["in_progress_job_id"].clone())
            .chain_err(|| "Unable to convert in_progress_job_id")?;

        for i in 0..job_store_size {
            let job_data = data["job_store"][i].clone();

            let job = Job::new_from_json(job_data).chain_err(
                || "Unable to create map reduce job from json",
            )?;

            self.job_queue.add_only_to_store(job.into()).chain_err(
                || "Unable to add mapReduceJob to QueuedWorkStore",
            )?;
        }

        // Add all queued jobs to the QueuedWorkStore queue.
        if let serde_json::Value::Array(job_ids) = data["queued_job_ids"].clone() {
            for serialized_job_id in job_ids {
                let job_id: String = serde_json::from_value(serialized_job_id).chain_err(
                    || "Unable to convert job_id",
                )?;

                self.job_queue.move_task_to_queue(job_id).chain_err(
                    || "Unable to move task to queue",
                )?;
            }
        }

        // Handle map reduce task queue.
        let task_queue_total: usize =
            serde_json::from_value(data["map_reduce_task_store_size"].clone())
                .chain_err(|| "Unable to convert map_reduce_task_store_size")?;

        for i in 0..task_queue_total {
            let task_data = data["map_reduce_task_store"][i].clone();

            let map_reduce_task = Task::new_from_json(task_data).chain_err(
                || "Unable to create Task",
            )?;

            self.task_queue
                .add_only_to_store(map_reduce_task.into())
                .chain_err(|| "Unable to add Task to QueuedWorkStore")?;
        }

        // Add all queued tasks to the QueuedWorkStore queue.
        if let serde_json::Value::Array(task_ids) = data["queued_map_reduce_task_ids"].clone() {
            for serialized_task_id in task_ids {
                let task_id: String = serde_json::from_value(serialized_task_id).chain_err(
                    || "Unable to convert task_id",
                )?;

                self.task_queue.move_task_to_queue(task_id).chain_err(
                    || "Unable to move task to queue",
                )?;
            }
        }

        self.available_workers = serde_json::from_value(data["available_workers"].clone())
            .chain_err(|| "Unable to convert available_workers")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queued_work_store::QueuedWork;
    use common::JobOptions;

    fn get_test_job_options() -> JobOptions {
        JobOptions {
            client_id: "client-1".to_owned(),
            binary_path: "/tmp/bin".to_owned(),
            input_directory: "/tmp/input".to_owned(),
            ..Default::default()
        }
    }

    struct TaskProcessorStub {
        map_tasks: Vec<Task>,
        reduce_tasks: Vec<Task>,
    }

    impl TaskProcessorStub {
        fn new(map_tasks: Vec<Task>, reduce_tasks: Vec<Task>) -> Self {
            TaskProcessorStub {
                map_tasks: map_tasks,
                reduce_tasks: reduce_tasks,
            }
        }
    }

    impl TaskProcessorTrait for TaskProcessorStub {
        fn create_map_tasks(&self, _job: &Job) -> Result<Vec<Task>> {
            Ok(self.map_tasks.clone())
        }
        fn create_reduce_tasks(
            &self,
            _job: &Job,
            _completed_map_tasks: &[&Task],
        ) -> Result<Vec<Task>> {
            Ok(self.reduce_tasks.clone())
        }
    }


    fn create_scheduler() -> Scheduler {
        Scheduler::new(Box::new(TaskProcessorStub::new(
            vec![
                Task::new_map_task(
                    "map-reduce1",
                    "/tmp/bin",
                    "input-1"
                ),
            ],
            Vec::new(),
        )))
    }

    #[test]
    fn test_schedule_job() {
        let mut scheduler = create_scheduler();
        let job = Job::new(get_test_job_options()).unwrap();
        scheduler.schedule_job(job.clone()).unwrap();
        assert_eq!(
            scheduler
                .get_in_progress_job_id()
                .unwrap(),
            job.id
        );
    }

    #[test]
    fn test_get_map_reduce_in_progress() {
        let mut scheduler = create_scheduler();
        // Assert that map reduce in progress starts as false.
        assert!(!scheduler.get_map_reduce_in_progress());
        scheduler
            .schedule_job(Job::new(get_test_job_options()).unwrap())
            .unwrap();
        assert!(scheduler.get_map_reduce_in_progress());
    }

    #[test]
    fn test_process_completed_task() {
        let job = Job::new(get_test_job_options()).unwrap();
        let map_task1 = Task::new_map_task(job.id.as_str(), "/tmp/bin", "input-1");

        let mut map_response = pb::MapResult::new();
        map_response.set_status(pb::ResultStatus::SUCCESS);

        map_response.mut_map_results().insert(
            0,
            "/tmp/worker/intermediate1"
                .to_owned(),
        );
        map_response.mut_map_results().insert(
            1,
            "/tmp/worker/intermediate2"
                .to_owned(),
        );

        let reduce_task1 = Task::new_reduce_task(
            job.id.as_str(),
            "/tmp/bin",
            0,
            vec!["/tmp/worker/intermediate1".to_owned()],
            "/tmp/output",
        );

        let mut reduce_response1 = pb::ReduceResult::new();
        reduce_response1.set_status(pb::ResultStatus::SUCCESS);

        let reduce_task2 = Task::new_reduce_task(
            job.id.as_str(),
            "/tmp/bin",
            1,
            vec!["/tmp/worker/intermediate2".to_owned()],
            "/tmp/output/",
        );

        let mut reduce_response2 = pb::ReduceResult::new();
        reduce_response2.set_status(pb::ResultStatus::SUCCESS);

        let mock_map_tasks = vec![map_task1.clone()];
        let mock_reduce_tasks = vec![reduce_task1.clone(), reduce_task2.clone()];

        let mut scheduler = Scheduler::new(Box::new(
            TaskProcessorStub::new(mock_map_tasks, mock_reduce_tasks),
        ));
        scheduler.schedule_job(job.clone()).unwrap();

        // Assert that the scheduler state starts as good.
        assert_eq!(
            job.id,
            scheduler
                .get_in_progress_job_id()
                .unwrap()
        );
        assert_eq!(1, scheduler.task_queue.queue_size());

        scheduler.pop_queued_task().unwrap();

        // Process response for map task
        scheduler
            .process_map_task_response(&map_task1.id, &map_response)
            .unwrap();

        {
            let job = scheduler
                .job_queue
                .get_work_by_id(&job.get_work_id())
                .unwrap();

            let map_task1 = scheduler
                .task_queue
                .get_work_by_id(&map_task1.get_work_id())
                .unwrap();

            assert_eq!(1, job.map_tasks_completed);
            assert_eq!(TaskStatus::Complete, map_task1.status);
            assert_eq!(
                "/tmp/worker/intermediate1",
                map_task1.map_output_files.get(&0).unwrap()
            );
            assert_eq!(
                "/tmp/worker/intermediate2",
                map_task1.map_output_files.get(&1).unwrap()
            );
            assert_eq!(2, scheduler.task_queue.queue_size());
        }

        scheduler.pop_queued_task().unwrap();
        scheduler.pop_queued_task().unwrap();

        // Process response for reduce task 1.
        scheduler
            .process_reduce_task_response(&reduce_task1.id, &reduce_response1)
            .unwrap();

        {
            let job = scheduler
                .job_queue
                .get_work_by_id(&job.get_work_id())
                .unwrap();

            let reduce_task1 = scheduler
                .task_queue
                .get_work_by_id(&reduce_task1.get_work_id())
                .unwrap();

            assert_eq!(1, job.reduce_tasks_completed);
            assert_eq!(TaskStatus::Complete, reduce_task1.status);

        }

        scheduler
            .process_reduce_task_response(&reduce_task2.id, &reduce_response2)
            .unwrap();

        let job = scheduler
            .job_queue
            .get_work_by_id(&job.get_work_id())
            .unwrap();

        assert!(
            scheduler
                .task_queue
                .get_work_by_id(&reduce_task2.get_work_id())
                .is_none()
        );
        assert_eq!(2, job.reduce_tasks_completed);
        assert_eq!(JobStatus::DONE, job.status);
    }
}