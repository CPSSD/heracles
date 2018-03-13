use std::net::SocketAddr;
use std::thread;

use futures::sync::{mpsc, oneshot};
use futures::{future, Future, Sink, Stream};
use lapin::channel::{BasicConsumeOptions, QueueDeclareOptions};
use lapin::client::{Client, ConnectionOptions};
use lapin::types::FieldTable;
use protobuf::Message;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use protobuf;

use heracles_proto::datatypes as pb;
use super::*;

const CHANNEL_BUFFER: usize = 1;
const AMQP_QUEUE_NAME: &str = "heracles_tasks";

pub struct Amqp;

impl Broker for Amqp {
    fn connect(addr: SocketAddr, handle: Handle) -> Result<BrokerConnection, BrokerError> {
        let (tx, rx) = mpsc::channel::<pb::Task>(CHANNEL_BUFFER);

        let queue_options = QueueDeclareOptions {
            durable: true,
            ..Default::default()
        };

        let setup_future = TcpStream::connect(&addr, &handle)
            .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()))
            .and_then(|(client, hearthbeat_future_fn)| {
                let heartbeat_client = client.clone();
                thread::spawn(move || {
                    Core::new()
                        .unwrap()
                        .run(hearthbeat_future_fn(&heartbeat_client))
                        .unwrap();
                });
                client.create_channel()
            })
            .and_then(move |channel| {
                channel
                    .queue_declare(AMQP_QUEUE_NAME, &queue_options, &FieldTable::new())
                    .and_then(|_| {
                        info!("AMQP queue `{}` successfully declared.", AMQP_QUEUE_NAME);
                        future::ok(channel)
                    })
            })
            .map_err(|e| e.context(BrokerErrorKind::ConnectionFailed));

        let conn_loop_future = setup_future.and_then(move |channel| {
            let ch = channel.clone();
            channel
                .basic_consume(
                    "",
                    AMQP_QUEUE_NAME,
                    &BasicConsumeOptions::default(),
                    &FieldTable::new(),
                )
                .map_err(|e| e.context(BrokerErrorKind::ConnectionFailed))
                .and_then(move |stream| {
                    stream
                        .map_err(|e| e.context(BrokerErrorKind::ConnectionFailed))
                        .for_each(move |message| {
                            ch.basic_ack(message.delivery_tag);
                            let sender = tx.clone();
                            let mut task = pb::Task::new();
                            future::done(task.merge_from(
                                &mut protobuf::CodedInputStream::from_bytes(&message.data),
                            )).map_err(|e| e.context(BrokerErrorKind::TaskDeserialisationFailed))
                                .and_then(move |_| {
                                    sender
                                        .send(task)
                                        .map_err(|e| e.context(BrokerErrorKind::ChannelSendFailed))
                                        .and_then(|_| future::ok(()))
                                })
                        })
                })
        });

        let error_future = oneshot::spawn(conn_loop_future, &handle);
        Ok(BrokerConnection {
            error_future: Box::new(error_future.map_err(|e| e.into())),
            handle: rx,
        })
    }
}
