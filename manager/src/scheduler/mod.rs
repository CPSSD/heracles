//! Module containing the `Scheduler`, a struct which manages the pipeline of the manager and links
//! all of the other components together.

use chrono::Utc;
use failure::*;
use futures::sync::mpsc;
use futures::*;
use lapin::channel::{BasicProperties, BasicPublishOptions, Channel};
use protobuf::Message;
use tokio::net::TcpStream;

use heracles_proto::datatypes::*;
use settings::SETTINGS;
use splitting;

/// Manages the entire data pipeline of the manager and links together all of the manager's
/// components.
pub struct Scheduler {
    broker_channel: Channel<TcpStream>,
    rx: mpsc::Receiver<Job>,
    tx: mpsc::Sender<Job>,
}

impl Scheduler {
    /// Construct a new `Scheduler`.
    ///
    /// Takes a handle to a [`heracles_manager_lib::broker::Broker`] which it uses to send
    /// [`Task`]s to workers for execution.
    pub fn new(broker_channel: Channel<TcpStream>) -> Result<Self, Error> {
        let (tx, rx) =
            mpsc::channel::<Job>(SETTINGS.read().unwrap().get("scheduler.input_queue_size")?);
        Ok(Scheduler {
            broker_channel,
            rx,
            tx,
        })
    }

    pub fn schedule(&self, _job: &Job) -> Result<String, SchedulerError> {
        unimplemented!()
    }

    pub fn cancel(&self, _job_id: &str) -> Result<(), SchedulerError> {
        unimplemented!()
    }

    fn process_job(&self, job: Job) -> impl Future<Item = Job, Error = Error> {
        lazy(|| done(splitting::map::split(&job))).and_then(|tasks| {
            future::join_all(tasks.into_iter().map(|task| self.process_task(task)))
        })
    }

    fn process_task<'a>(&'a self, mut task: Task) -> impl Future<Item = Task, Error = Error> + 'a {
        // This unwrap should be safe as we set a default for this value.
        let queue_name = SETTINGS.read().unwrap().get("broker_queue_name").unwrap();
        let task_id = task.get_id().to_string();
        lazy(|| done(task.write_to_bytes()))
            .map_err(move |e| e.context(SchedulerError::TaskSerialisationFailure { task_id }))
            .from_err()
            .and_then(move |bytes| {
                self.broker_channel
                    .basic_publish(
                        "",
                        queue_name,
                        &bytes,
                        &BasicPublishOptions::default(),
                        BasicProperties::default(),
                    )
                    .from_err()
            })
            .and_then(|ack| {
                if let Some(completed) = ack {
                    if completed {
                        task.set_status(TaskStatus::TASK_DONE);
                    } else {
                        task.set_status(TaskStatus::TASK_FAILED);
                    }
                } else {
                    task.set_status(TaskStatus::TASK_UNKNOWN);
                    panic!("Queue was not confirm queue. This should not happen.");
                }
                task.set_time_done(Utc::now().timestamp() as u64);
                future::ok(task)
            })
    }
}

#[derive(Debug, Fail)]
pub enum SchedulerError {
    #[fail(display = "failed to serialise task with id {}", task_id)]
    TaskSerialisationFailure { task_id: String },
}
