use std::net::SocketAddr;

use futures::sync::{mpsc, oneshot};
use futures::{future, Future, Stream};
use lapin::channel::{BasicProperties, BasicPublishOptions, QueueDeclareOptions};
use lapin::client::{Client, ConnectionOptions};
use lapin::types::FieldTable;
use protobuf::Message;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;

use cerberus_proto::datatypes::Task;
use super::*;

const CHANNEL_BUFFER: usize = 1;
const AMQP_QUEUE_NAME: &str = "heracles_tasks";

pub struct Amqp;

impl Broker for Amqp {
    fn connect(addr: SocketAddr, handle: Handle) -> Result<BrokerConnection, Error> {
        let (tx, rx) = mpsc::channel::<Task>(CHANNEL_BUFFER);
        let queue_options = QueueDeclareOptions {
            durable: true,
            ..Default::default()
        };

        let setup_future = TcpStream::connect(&addr, &handle)
            .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()))
            .and_then(|(client, _)| client.create_channel())
            .and_then(move |channel| {
                channel
                    .queue_declare(AMQP_QUEUE_NAME, &queue_options, &FieldTable::new())
                    .and_then(|_| {
                        info!("AMQP queue `{}` successfully declared.", AMQP_QUEUE_NAME);
                        future::ok(channel)
                    })
            })
            .map_err(|e| e.context(BrokerErrorKind::ConnectionFailed));

        let conn_loop_future = setup_future.and_then(|channel| {
            rx.map_err(|_| unreachable!("receiver does not error"))
                .for_each(move |task| {
                    let ch = channel.clone();
                    future::done(task.write_to_bytes())
                        .map_err(|e| e.context(BrokerErrorKind::TaskSerialisationFailed))
                        .and_then(move |bytes| {
                            ch.basic_publish(
                                "",
                                AMQP_QUEUE_NAME,
                                &bytes,
                                &BasicPublishOptions::default(),
                                BasicProperties::default(),
                            ).and_then(move |_| {
                                    debug!(
                                        "Task of length {} bytes sent to queue `{}`.",
                                        bytes.len(),
                                        AMQP_QUEUE_NAME
                                    );
                                    future::ok(())
                                })
                                .map_err(|e| e.context(BrokerErrorKind::ConnectionFailed))
                        })
                })
        });

        let error_future = oneshot::spawn(conn_loop_future, &handle);
        Ok(BrokerConnection {
            error_future: Box::new(error_future.map_err(|e| e.into())),
            handle: tx,
        })
    }
}
