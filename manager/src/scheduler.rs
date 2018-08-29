//! Module containing the `Scheduler`, a struct which manages the pipeline of the manager and links
//! all of the other components together.
use std::sync::{Mutex, Arc};

use chrono::Utc;
use failure::*;
use futures::*;
use futures::sync::mpsc;
use uuid::Uuid;
use futures::Future;

use heracles_proto::datatypes::*;
use splitting;
use broker::BrokerConnection;
use state::State;
use settings::SETTINGS;

/// Manages the entire data pipeline of the manager and links together all of the manager's
/// components.
#[derive(Clone)]
pub struct Scheduler {
    // broker: Arc<BrokerConnection + Sync + Send>,
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
    // pub fn new<B: ?Sized>(broker: &mut B, store: Arc<State + Send + Sync>) -> Result<Self, Error>
    //     where B: IntoFuture<Item=BrokerConnection, Error=Error>
    // {
        let (tx, rx) =
            mpsc::channel::<Job>(SETTINGS.read().unwrap().get("scheduler.input_queue_size")?);
        Ok(Scheduler {
            broker: broker,
            store: store,
            rx: Arc::new(Mutex::new(Some(rx))),
            tx: Arc::new(Mutex::new(Some(tx))),
        })
    }

    pub fn schedule<'a>(&'a self, req: Job) -> Result<String, Error> {
        let sch = self.clone();

        let mut job = req.clone();

        let id = Uuid::new_v4().to_string();
        job.set_id(id.clone());
        // TODO: Scheduling time

        sch.store.save_job(&job.clone())?;

        sch.tx.lock().unwrap().take().unwrap().send(job.clone()).wait()?;

        info!("have send the job to be executed. Returning the ID");

        Ok(id)
    }

    pub fn cancel<'a>(&'a self, _job_id: &str) -> Result<(), Error> {
        unimplemented!()
    }

    // pub fn run<B>(&self, broker: B) -> impl Future<Item = (), Error = ()> + 'static
    //     where B: IntoFuture<Item=BrokerConnection, Error=Error>
    // {
    pub fn run(&self) -> impl Future<Item = (), Error = ()> + 'static {
        let sch = self.clone();

        // broker.and_then(|bc| {
            self.rx.lock().unwrap().take().unwrap()
                    .map_err(|_| unreachable!("should never happen"))
                    .for_each(move |job| process_job(job, sch.clone().broker.clone(), sch.clone().store.clone()))
                    .map_err(|e| error!("{}", e))
        // })
    }
}

fn process_job(job: Job, broker: Arc<BrokerConnection>, store: Arc<State>) -> impl Future<Item = (), Error = Error> + 'static {
    // TODO: Refactor this ugly code. This should not be cloned so many times.
    let job1 = job.clone();
    let job2 = job.clone();
    let job3 = job.clone();

    let broker1 = broker.clone();
    let broker2 = broker.clone();

    let store1 = store.clone();
    let store2 = store.clone();
    let store3 = store.clone();

    info!("Begining the job processing pipeline");

    lazy(move || done(splitting::map::split(&job1)))
        .and_then(move |tasks| run_tasks(tasks, broker1.clone(), store1.clone()))
        .and_then(move |_| future::ok(splitting::reduce::split(&job2)))
        .and_then(move |tasks| run_tasks(tasks, broker2.clone(), store2.clone()))
        .and_then(move |_| {
            // mark job as done
            store3.clone().save_job(&job3).unwrap();
            future::ok(())
        })
 }

fn run_tasks(tasks: Vec<Task>, broker: Arc<BrokerConnection>, store: Arc<State>) -> impl Future<Item = (), Error = Error> {
    // Normally we would do `.into_iter()` on the task, but it looks like there is a problem
    // with it currently. This issue describes the error we are having:
    //      https://github.com/rust-lang/rust/issues/49926
    let mut task_futures = vec![];
    for mut task in tasks {
        task_futures.push(process_task(task.clone(), broker.clone(), store.clone()));
    }
    future::join_all(task_futures).and_then(|_| future::ok(()))
}

fn process_task(mut task: Task, broker: Arc<BrokerConnection>, store: Arc<State>) -> impl Future<Item = (), Error = Error> + 'static {
    task.set_time_started(Utc::now().timestamp() as u64);
    task.set_status(TaskStatus::TASK_IN_PROGRESS);

    store.save_task(&task).unwrap();

    info!("Sending task to broker");

    broker.send(task.clone())
        // .map_err(|e| e.context(SchedulerError::BrokerSendFailure))
        // .from_err()
        .and_then(move |ack| {
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
            store.save_task(&task).unwrap();
            future::ok(())
        })
}

#[derive(Debug, Fail, Copy, Clone)]
pub enum SchedulerError {
    #[fail(display = "failed to split job into map tasks")]
    MapSplitFailure,
    #[fail(display = "failed to send task to broker")]
    BrokerSendFailure,
    #[fail(display = "error receiving")]
    RxFailure,
}
