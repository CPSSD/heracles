use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use lapin::channel::{BasicProperties, BasicPublishOptions, Channel, QueueDeclareOptions};
use lapin::client::{Client, ConnectionOptions};
use lapin::types::FieldTable;
use protobuf::Message;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio;

use super::*;
use settings::SETTINGS;

pub struct AMQPBrokerConnection {
    channel: Arc<Channel<TcpStream>>,
}

impl BrokerConnection for AMQPBrokerConnection {
    /// Sends a `Task` to the broker.
    ///
    /// The `Option<bool>` returned represents whether the message was acked (`Some(true)`), nacked
    /// (`Some(false)`), or the queue is not a confirm queue (`None`).
    fn send(&self, task: Task) -> Box<Future<Item = Option<bool>, Error = Error> + Send + 'static> {
        let task_id = task.get_id().to_string();
        let ch = self.channel.clone();
        let queue_name: String = SETTINGS.read().unwrap().get("broker.queue_name").unwrap();

        let ret = future::lazy(move || future::done(task.write_to_bytes()))
            .map_err(|e| e.context(BrokerError::TaskSerialisationFailure { task_id }))
            .from_err()
            .and_then(move |bytes| {
                info!("publishing task");

                ch.basic_publish(
                    "",
                    &queue_name.as_str(),
                    &bytes,
                    &BasicPublishOptions::default(),
                    BasicProperties::default(),
                ).from_err()
            });
        Box::new(ret)
    }
}

// pub fn connect(addr: SocketAddr) -> Result<AMQPBrokerConnection, Error> {
//     let queue_name: String = SETTINGS.read().unwrap().get("broker.queue_name").unwrap();
//     let queue_options = QueueDeclareOptions {
//         durable: true,
//         ..Default::default()
//     };

//     tokio::run(TcpStream::connect(&addr)
//         .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()))
//         .and_then(|(client, _)| client.create_channel())
//         .and_then(move |channel| {
//             channel
//                 .queue_declare(&queue_name.as_str(), &queue_options, &FieldTable::new())
//                 .and_then(move |_| {
//                     info!("AMQP queue `{}` successfully declared.", queue_name);
//                     future::ok(channel)
//                 })
//         })
//         .map_err(|e| e.context(BrokerError::ConnectionFailed).into())
//         .and_then(move |channel| {
//             AMQPBrokerConnection {
//                 channel: Arc::new(channel),
//             }
//         }))
// }


pub fn connect(addr: SocketAddr) -> Result<AMQPBrokerConnection, Error> {
    let queue_name: String = SETTINGS.read().unwrap().get("broker.queue_name").unwrap();
    let queue_options = QueueDeclareOptions {
        durable: true,
        ..Default::default()
    };

    // I assume this can't actually be here.
    // let mut bc = AMQPBrokerConnection{
    //     channel: Arc::new(),
    // };

    let broker_conn = TcpStream::connect(&addr)
        .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()))
        .and_then(|(client, _)| client.create_channel())
        .and_then(move |channel| {
            channel
                .queue_declare(&queue_name.as_str(), &queue_options, &FieldTable::new())
                .and_then(move |_| {
                    future::ok(channel)
                })
        })
        // .map_err(|e| e.context(BrokerError::ConnectionFailed).into())
        .map_err(|e| error!("{}", e))
        .and_then(|channel| {
            // Add the channel to the broker connection
            // bc.channel = Arc::new(channel);
            future::ok(())
        });

    // This is definatelly bad, but we can't just do run because it will block
    thread::spawn(move || {
        tokio::run(broker_conn);
    });

    Ok(AMQPBrokerConnection{
        channel: Arc::new(broker_conn),
    })
}