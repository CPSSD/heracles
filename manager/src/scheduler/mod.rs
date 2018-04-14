//! Module containing the `Scheduler`, a struct which manages the pipeline of the manager and links
//! all of the other components together.

use chrono::Utc;
use failure::*;
use futures::sync::mpsc;
use futures::*;
use lapin::channel::{BasicProperties, BasicPublishOptions, Channel};
use protobuf::Message;
use tokio::net::TcpStream;

use std::sync::Arc;

use heracles_proto::datatypes::*;
use settings::SETTINGS;
use splitting;
use broker::BrokerConnection;
use state::State;

/// Manages the entire data pipeline of the manager and links together all of the manager's
/// components.
pub struct Scheduler {
    broker: Arc<BrokerConnection>,
    store: Arc<State>,
}

impl Scheduler {
    /// Construct a new `Scheduler`.
    ///
    /// Takes a handle to a [`heracles_manager_lib::broker::Broker`] which it uses to send
    /// [`Task`]s to workers for execution.
    pub fn new(broker: Arc<BrokerConnection>, store: Arc<State>) -> Result<Self, Error> {
        Ok(Scheduler {
            broker,
            store,
        })
    }

    pub fn schedule(&self, _job: &Job) -> Result<String, SchedulerError> {
        unimplemented!()
    }

    pub fn cancel(&self, _job_id: &str) -> Result<(), SchedulerError> {
        unimplemented!()
    }

    fn process_job<'a>(&'a self, job: Job) -> impl Future<Item = Job, Error = Error> {
        lazy(|| done(splitting::map::split(&job))).and_then(|tasks| {
            future::join_all(tasks.into_iter().map(|task| {
                self.process_task(task)
            }))
            .and_then(move |_| {
               // TODO: Run reduce
               future::ok(job)
            })
        })
    }

    fn process_task<'a>(&'a self, mut task: Task) -> impl Future<Item = Task, Error = Error> + 'a {
        task.set_time_started(Utc::now().timestamp() as u64);
        task.set_status(TaskStatus::TASK_IN_PROGRESS);
        self.store.save_task(&task);

        self.broker.send(&task)
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
                future::ok(task)
            })
    }
}

#[derive(Debug, Fail)]
pub enum SchedulerError {
    #[fail(display = "failed to send task to broker")]
    BrokerSendFailure,
}
