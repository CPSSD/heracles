use cerberus_proto::mrworker::WorkerStatusResponse_OperationStatus as OperationStatus;
use cerberus_proto::mrworker::{PerformMapRequest, PerformReduceRequest};
use errors::*;
use mapreduce_job::MapReduceJob;
use mapreduce_tasks::{MapReduceTask, MapReduceTaskStatus, TaskProcessorTrait, TaskType};
use queued_work_store::QueuedWorkStore;
use std::{thread, time};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use worker_interface::WorkerInterface;
use worker_manager::{Worker, WorkerManager};

const SCHEDULING_LOOP_INTERVAL_MS: u64 = 100;


pub struct MapReduceScheduler {
    map_reduce_job_queue: QueuedWorkStore<MapReduceJob>,
    map_reduce_task_queue: QueuedWorkStore<MapReduceTask>,

    map_reduce_in_progress: bool,
    in_progress_map_reduce_id: Option<String>,

    available_workers: u32,
    task_processor: Box<TaskProcessorTrait + Send>,
}

impl MapReduceScheduler {
    pub fn new(task_processor: Box<TaskProcessorTrait + Send>) -> Self {
        let job_queue: QueuedWorkStore<MapReduceJob> = QueuedWorkStore::new();
        let task_queue: QueuedWorkStore<MapReduceTask> = QueuedWorkStore::new();
        MapReduceScheduler {
            map_reduce_job_queue: job_queue,
            map_reduce_task_queue: task_queue,

            map_reduce_in_progress: false,
            in_progress_map_reduce_id: None,

            available_workers: 0,
            task_processor: task_processor,
        }
    }

    fn process_next_map_reduce(&mut self) -> Result<()> {
        match self.map_reduce_job_queue.pop_queue_top() {
            Some(map_reduce_job) => {
                self.map_reduce_in_progress = true;
                self.in_progress_map_reduce_id =
                    Some(map_reduce_job.get_map_reduce_id().to_owned());
                let map_tasks: Vec<MapReduceTask> =
                    self.task_processor.create_map_tasks(map_reduce_job)?;
                for task in map_tasks {
                    self.map_reduce_task_queue
                        .add_to_store(Box::new(task))
                        .chain_err(|| "Error adding map reduce task to queue")?;
                }
                Ok(())
            }
            None => Err("no queued map reduce".into()),
        }
    }

    pub fn get_map_reduce_in_progress(&self) -> bool {
        self.map_reduce_in_progress
    }

    pub fn get_in_progress_map_reduce_id(&self) -> Option<String> {
        self.in_progress_map_reduce_id.clone()
    }

    pub fn get_map_reduce_job_queue_size(&self) -> usize {
        self.map_reduce_job_queue.queue_size()
    }

    pub fn get_map_reduce_task_queue_size(&self) -> usize {
        self.map_reduce_task_queue.queue_size()
    }

    pub fn schedule_map_reduce(&mut self, map_reduce_job: MapReduceJob) -> Result<()> {
        self.map_reduce_job_queue
            .add_to_store(Box::new(map_reduce_job))
            .chain_err(|| "Error adding map reduce job to queue.")?;
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

    pub fn get_mapreduce_status(&self, mapreduce_id: String) -> Result<&MapReduceJob> {
        let result = self.map_reduce_job_queue.get_work_by_id(&mapreduce_id);
        match result {
            None => Err("There was an error getting the result".into()),
            Some(job) => Ok(job),
        }
    }

    pub fn get_mapreduce_client_status(&self, client_id: String) -> Result<Vec<&MapReduceJob>> {
        self.map_reduce_job_queue.get_work_bucket_items(&client_id)
    }

    pub fn pop_queued_map_reduce_task(&mut self) -> Option<&mut MapReduceTask> {
        self.map_reduce_task_queue.pop_queue_top()
    }

    pub fn unschedule_task(&mut self, task_id: String) -> Result<()> {
        self.map_reduce_task_queue
            .move_task_to_queue(task_id.to_owned())
            .chain_err(|| "Error unscheduling map reduce task")?;

        let task = self.map_reduce_task_queue
            .get_work_by_id_mut(&task_id)
            .chain_err(|| "Error unschuling map reduce task")?;

        task.set_assigned_worker_id(String::new());
        task.set_status(MapReduceTaskStatus::Queued);

        Ok(())
    }
}

#[derive(Clone)]
struct SchedulerResources {
    scheduler_arc: Arc<Mutex<MapReduceScheduler>>,
    worker_manager_arc: Arc<Mutex<WorkerManager>>,
    worker_interface_arc: Arc<RwLock<WorkerInterface>>,
}

fn handle_assign_task_failure(
    scheduler_resources: &SchedulerResources,
    worker_id: &str,
    task_id: &str,
) {
    match scheduler_resources.worker_manager_arc.lock() {
        Ok(mut worker_manager) => {
            let worker_option = worker_manager.get_worker(worker_id);
            match worker_option {
                Some(worker) => {
                    worker.set_scheduling_in_progress(false);
                    worker.set_current_task_id(String::new());
                    worker.set_operation_status(OperationStatus::UNKNOWN);
                }
                None => {
                    error!("Error reverting worker state on task failure: Worker not found");
                }
            }
        }
        Err(err) => {
            error!("Error reverting worker state on task failure: {}", err);
        }
    }

    match scheduler_resources.scheduler_arc.lock() {
        Ok(mut scheduler) => {
            let result = scheduler.unschedule_task(task_id.to_owned());
            if let Err(err) = result {
                error!("Error reverting task state on task failure: {}", err);
            }
        }
        Err(err) => {
            error!("Error reverting worker state on task failure: {}", err);
        }
    }
}


fn assign_worker_map_task(
    scheduler_resources: SchedulerResources,
    worker_id: String,
    task_id: String,
    map_task: PerformMapRequest,
) {
    thread::spawn(move || {
        let read_guard = scheduler_resources.worker_interface_arc.read();
        match read_guard {
            Ok(worker_interface) => {
                let result = worker_interface.schedule_map(map_task, &worker_id);
                if let Err(err) = result {
                    error!("Error assigning worker task: {}", err);
                    handle_assign_task_failure(&scheduler_resources, &worker_id, &task_id);
                }
            }
            Err(err) => {
                error!("Error assigning worker task: {}", err);
                handle_assign_task_failure(&scheduler_resources, &worker_id, &task_id);
            }
        }
    });
}

fn assign_worker_reduce_task(
    scheduler_resources: SchedulerResources,
    worker_id: String,
    task_id: String,
    reduce_task: PerformReduceRequest,
) {
    thread::spawn(move || {
        let read_guard = scheduler_resources.worker_interface_arc.read();
        match read_guard {
            Ok(worker_interface) => {
                let result = worker_interface.schedule_reduce(reduce_task, &worker_id);
                if let Err(err) = result {
                    error!("Error assigning worker task: {}", err);
                    handle_assign_task_failure(&scheduler_resources, &worker_id, &task_id);
                }
            }
            Err(err) => {
                error!("Error assigning worker task: {}", err);
                handle_assign_task_failure(&scheduler_resources, &worker_id, &task_id);
            }
        }
    });
}

// TODO(conor) : Refactor MapReduceTask so they already include built requests.
// There is no smart way to handle errors here.
fn build_map_request(task: &MapReduceTask) -> PerformMapRequest {
    let mut map_request = PerformMapRequest::new();
    map_request.set_input_file_path(task.get_input_files()[0].to_owned());
    map_request.set_mapper_file_path(task.get_binary_path().to_owned());
    map_request
}

// TODO(conor) : Refactor MapReduceTask so they already include built requests.
// There is no smart way to handle errors here.
fn build_reduce_request(task: &MapReduceTask) -> PerformReduceRequest {
    let mut reduce_request = PerformReduceRequest::new();
    reduce_request.set_intermediate_key(task.get_input_key().unwrap());
    for input_file in task.get_input_files() {
        reduce_request.mut_input_file_paths().push(
            input_file.to_owned(),
        );
    }
    reduce_request.set_reducer_file_path(task.get_binary_path().to_owned());
    reduce_request
}

fn assign_worker_task<S: Into<String>>(
    scheduler_resources: SchedulerResources,
    worker_id: S,
    task_id: S,
    task: &MapReduceTask,
) {
    match task.get_task_type() {
        TaskType::Map => {
            let map_request = build_map_request(task);
            assign_worker_map_task(
                scheduler_resources,
                worker_id.into(),
                task_id.into(),
                map_request,
            )
        }
        TaskType::Reduce => {
            let reduce_request = build_reduce_request(task);
            assign_worker_reduce_task(
                scheduler_resources,
                worker_id.into(),
                task_id.into(),
                reduce_request,
            );
        }
    }
}

fn do_scheduling_loop_step(
    scheduler_resources: &SchedulerResources,
    mut scheduler: MutexGuard<MapReduceScheduler>,
    mut worker_manager: MutexGuard<WorkerManager>,
) {
    let mut available_workers: Vec<&mut Worker> = worker_manager.get_available_workers();
    while scheduler.get_map_reduce_task_queue_size() > 0 {
        if available_workers.is_empty() {
            break;
        }

        let task: &mut MapReduceTask = {
            match scheduler.pop_queued_map_reduce_task() {
                Some(task) => task,
                None => {
                    error!("Unexpected error getting queued map reduce task");
                    break;
                }
            }
        };

        let worker: &mut Worker = {
            match available_workers.pop() {
                Some(worker) => worker,
                None => {
                    error!("Unexpected error getting available worker");
                    break;
                }
            }
        };

        worker.set_scheduling_in_progress(true);
        worker.set_current_task_id(task.get_task_id());
        worker.set_operation_status(OperationStatus::IN_PROGRESS);

        task.set_assigned_worker_id(worker.get_worker_id());
        task.set_status(MapReduceTaskStatus::InProgress);

        assign_worker_task(
            scheduler_resources.clone(),
            worker.get_worker_id(),
            task.get_task_id(),
            task,
        );
    }
}

pub fn run_scheduling_loop(
    worker_interface_arc: Arc<RwLock<WorkerInterface>>,
    scheduler_arc: Arc<Mutex<MapReduceScheduler>>,
    worker_manager_arc: Arc<Mutex<WorkerManager>>,
) {
    thread::spawn(move || loop {
        thread::sleep(time::Duration::from_millis(SCHEDULING_LOOP_INTERVAL_MS));

        let scheduler = {
            match scheduler_arc.lock() {
                Ok(scheduler) => scheduler,
                Err(e) => {
                    error!("Error obtaining scheduler: {}", e);
                    continue;
                }
            }
        };

        let worker_manager = {
            match worker_manager_arc.lock() {
                Ok(worker_manager) => worker_manager,
                Err(e) => {
                    error!("Error obtaining worker manager: {}", e);
                    continue;
                }
            }
        };

        do_scheduling_loop_step(
            &SchedulerResources {
                scheduler_arc: Arc::clone(&scheduler_arc),
                worker_manager_arc: Arc::clone(&worker_manager_arc),
                worker_interface_arc: Arc::clone(&worker_interface_arc),
            },
            scheduler,
            worker_manager,
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use mapreduce_tasks::TaskType;

    struct TaskProcessorStub {
        map_reduce_tasks: Vec<MapReduceTask>,
    }

    impl TaskProcessorTrait for TaskProcessorStub {
        fn create_map_tasks(&self, _map_reduce_job: &MapReduceJob) -> Result<Vec<MapReduceTask>> {
            Ok(self.map_reduce_tasks.clone())
        }
        fn create_reduce_tasks(
            &self,
            _map_reduce_job: &MapReduceJob,
            _completed_map_tasks: &[MapReduceTask],
        ) -> Result<Vec<MapReduceTask>> {
            Ok(Vec::new())
        }
    }


    fn create_map_reduce_scheduler() -> MapReduceScheduler {
        MapReduceScheduler::new(Box::new(TaskProcessorStub {
            map_reduce_tasks: vec![
                MapReduceTask::new(
                    TaskType::Map,
                    "map-reduce1",
                    "/tmp/bin",
                    None,
                    vec!["input-1".to_owned()]
                ).unwrap(),
            ],
        }))
    }

    #[test]
    fn test_schedule_map_reduce() {
        let mut map_reduce_scheduler = create_map_reduce_scheduler();
        let map_reduce_job = MapReduceJob::new("client-1", "/tmp/bin", "/tmp/input");
        map_reduce_scheduler
            .schedule_map_reduce(map_reduce_job.clone())
            .unwrap();
        assert_eq!(
            map_reduce_scheduler
                .get_in_progress_map_reduce_id()
                .unwrap(),
            map_reduce_job.get_map_reduce_id()
        );
    }

    #[test]
    fn test_get_map_reduce_in_progress() {
        let mut map_reduce_scheduler = create_map_reduce_scheduler();
        // Assert that map reduce in progress starts as false.
        assert!(!map_reduce_scheduler.get_map_reduce_in_progress());
        map_reduce_scheduler
            .schedule_map_reduce(MapReduceJob::new("client-1", "/tmp/bin", "/tmp/input"))
            .unwrap();
        assert!(map_reduce_scheduler.get_map_reduce_in_progress());
    }
}
