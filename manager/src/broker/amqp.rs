use std::net::SocketAddr;

use lapin::channel::{Channel, QueueDeclareOptions};
use lapin::client::{Client, ConnectionOptions};
use lapin::types::FieldTable;
use tokio::net::TcpStream;
use tokio::prelude::*;

use super::*;

const AMQP_QUEUE_NAME: &str = "heracles_tasks";

pub fn connect(addr: SocketAddr) -> impl Future<Item = Channel<TcpStream>, Error = Error> {
    let queue_options = QueueDeclareOptions {
        durable: true,
        ..Default::default()
    };

    TcpStream::connect(&addr)
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
        .map_err(|e| e.context(BrokerErrorKind::ConnectionFailed).into())
}
