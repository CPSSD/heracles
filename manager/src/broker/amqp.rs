use std::net::SocketAddr;
use std::sync::Arc;

use lapin::channel::{BasicProperties, BasicPublishOptions, Channel, QueueDeclareOptions};
use lapin::client::{Client, ConnectionOptions};
use lapin::types::FieldTable;
use protobuf::Message;
use tokio::net::TcpStream;
use tokio::prelude::*;

use super::*;
use settings::SETTINGS;

pub struct AMQPBrokerConnection {
    channel: Arc<Channel<TcpStream>>,
    queue_name: String,
}

impl BrokerConnection for AMQPBrokerConnection {
    /// Sends a `Task` to the broker.
    ///
    /// The `Option<bool>` returned represents whether the message was acked (`Some(true)`), nacked
    /// (`Some(false)`), or the queue is not a confirm queue (`None`).
    fn send(&self, task: Task) -> Box<Future<Item = Option<bool>, Error = Error> + Send + 'static> {
        let task_id = task.get_id().to_string();
        let ret = future::lazy(move || future::done(task.write_to_bytes()))
            .map_err(|e| e.context(BrokerError::TaskSerialisationFailure { task_id }))
            .from_err()
            .and_then(move |bytes| {
                self.channel
                    .clone()
                    .basic_publish(
                        "",
                        &self.queue_name,
                        &bytes,
                        &BasicPublishOptions::default(),
                        BasicProperties::default(),
                    )
                    .from_err()
            });
        Box::new(ret)
    }
}

pub fn connect(addr: SocketAddr) -> impl Future<Item = AMQPBrokerConnection, Error = Error> {
    let queue_name = SETTINGS.read().unwrap().get("broker_queue_name").unwrap();
    let queue_options = QueueDeclareOptions {
        durable: true,
        ..Default::default()
    };

    TcpStream::connect(&addr)
        .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()))
        .and_then(|(client, _)| client.create_channel())
        .and_then(move |channel| {
            channel
                .queue_declare(queue_name, &queue_options, &FieldTable::new())
                .and_then(move |_| {
                    info!("AMQP queue `{}` successfully declared.", queue_name);
                    future::ok(channel)
                })
        })
        .map_err(|e| e.context(BrokerError::ConnectionFailed).into())
        .and_then(move |channel| {
            future::ok(AMQPBrokerConnection {
                channel: Arc::new(channel),
                queue_name: queue_name.to_string(),
            })
        })
}
