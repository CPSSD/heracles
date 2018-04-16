//! Module containing the `Scheduler`, a struct which manages the pipeline of the manager and links
//! all of the other components together.
use std::sync::{Mutex, Arc};

use chrono::Utc;
use failure::*;
use futures::*;
use futures::sync::mpsc;
use uuid::Uuid;

use heracles_proto::datatypes::*;
use splitting;
use broker::BrokerConnection;
use state::State;
use settings::SETTINGS;

/// Manages the entire data pipeline of the manager and links together all of the manager's
/// components.
pub struct Scheduler {
    broker: Arc<BrokerConnection + Sync + Send>,
    store: Arc<State + Sync + Send>,
    rx: Arc<Mutex<Option<mpsc::Receiver<Job>>>>,
    tx: Arc<Mutex<Option<mpsc::Sender<Job>>>>,
}

impl Scheduler {
    /// Construct a new `Scheduler`.
    ///
    /// Takes a handle to a [`heracles_manager_lib::broker::Broker`] which it uses to send
    /// [`Task`]s to workers for execution.
    pub fn new(broker: Arc<BrokerConnection + Send + Sync>, store: Arc<State + Send + Sync>) -> Result<Self, Error> {
        let (tx, rx) =
            mpsc::channel::<Job>(SETTINGS.read().unwrap().get("scheduler.input_queue_size")?);
        Ok(Scheduler {
            broker: broker,
            store: store,
            rx: Arc::new(Mutex::new(Some(rx))),
            tx: Arc::new(Mutex::new(Some(tx))),
        })
    }

    pub fn schedule(&self, req: Job) -> Result<String, SchedulerError> {
        let mut job = req.clone();

        let id = Uuid::new_v4().to_string();
        job.set_id(id.clone());
        // TODO: Scheduling time

        self.store.save_job(&job.clone()).unwrap();

        // self.tx.borrow_mut().take().unwrap().send(job.clone());

        self.tx.lock().unwrap().take().unwrap().clone().send(job.clone());

        Ok(id)
    }

    pub fn cancel(&self, _job_id: &str) -> Result<(), SchedulerError> {
        unimplemented!()
    }

    pub fn run(&self) -> impl Future<Item = (), Error = ()> {
        self.rx
            .clone()
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .map_err(|_| unreachable!("should never happen"))
            .for_each(|job| self.process_job(job))
            .map_err(|_| panic!("should not happen"))
    }

    fn process_job(&self, job: Job) -> impl Future<Item = (), Error = Error> + 'static {
        // TODO: Refactor this ugly code. This should not be cloned so many times.
        let job1 = job.clone();
        let job2 = job.clone();
        let job3 = job.clone();
        lazy(|| done(splitting::map::split(&job1)))
            .and_then(|tasks| self.run_tasks(tasks))
            .and_then(|_| future::ok(splitting::reduce::split(&job2)))
            .and_then(|tasks| self.run_tasks(tasks))
            .and_then(|_| {
                // mark job as done

                self.store.save_job(&job3);
                future::ok(())
            })
    }

    fn run_tasks(&self, tasks: Vec<Task>) -> impl Future<Item = (), Error = Error> + 'static {
        // Normally we would do `.into_iter()` on the task, but it looks like there is a problem
        // with it currently. This issue describes the error we are having:
        //      https://github.com/rust-lang/rust/issues/49926
        let mut task_futures = vec![];
        for mut task in tasks {
            task_futures.push(self.process_task(task.clone()));
        }
        future::join_all(task_futures).and_then(|_| future::ok(()))
    }

    fn process_task(&self, mut task: Task) -> impl Future<Item = (), Error = Error> + 'static {
        task.set_time_started(Utc::now().timestamp() as u64);
        task.set_status(TaskStatus::TASK_IN_PROGRESS);
        self.store.save_task(&task);

        self.broker.send(task.clone())
            // .map_err(|e| e.context(SchedulerError::BrokerSendFailure))
            // .from_err()
            .and_then(|ack| {
                if let Some(completed) = ack {
                    if completed {
                        task.set_status(TaskStatus::TASK_DONE);
                    } else {
                        task.set_status(TaskStatus::TASK_FAILED);
                    }
                } else {
                    task.set_status(TaskStatus::TASK_UNKNOWN);
                    panic!("ack of task failed. this should not happen");
                }
                task.set_time_done(Utc::now().timestamp() as u64);
                self.store.save_task(&task);
                future::ok(())
            })
    }
}

// fn run_tasks(tasks: Vec<Task>) -> impl Future<Item = (), Error = Error> {
//     // Normally we would do `.into_iter()` on the task, but it looks like there is a problem
//     // with it currently. This issue describes the error we are having:
//     //      https://github.com/rust-lang/rust/issues/49926
//     let mut task_futures = vec![];
//     for mut task in tasks {
//         task_futures.push(self.process_task(task.clone()));
//     }
//     future::join_all(task_futures).and_then(|_| future::ok(()))
// }

// fn process_task()

#[derive(Debug, Fail)]
pub enum SchedulerError {
    #[fail(display = "failed to split job into map tasks")]
    MapSplitFailure,
    #[fail(display = "failed to send task to broker")]
    BrokerSendFailure,
    #[fail(display = "error receiving")]
    RxFailure,
}
