pub mod amqp;

use failure::*;
use futures::Future;

use heracles_proto::datatypes::Task;

pub trait BrokerConnection: Send + Sync {
    fn send(&self, Task) -> Box<Future<Item = Option<bool>, Error = Error> + Send + 'static>;
}

#[derive(Debug, Fail)]
pub enum BrokerError {
    #[fail(display = "Failed to connect to message broker server.")]
    ConnectionFailed,
    #[fail(display = "Failed to serialise task with ID {},", task_id)]
    TaskSerialisationFailure { task_id: String },
}
