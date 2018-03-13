extern crate chrono;
extern crate failure;
extern crate fern;
extern crate futures;
extern crate heracles_manager_lib;
#[macro_use]
extern crate log;
extern crate tokio_core;

use failure::*;
use futures::future;
use tokio_core::reactor::Core;

use heracles_manager_lib::broker::Broker;
use heracles_manager_lib::settings::SETTINGS;
use heracles_manager_lib::{broker, optparse, settings};

fn main() {
    if let Err(err) = run() {
        error!("{}", err);
        for cause in err.causes().skip(1) {
            error!("Caused by: {}", cause);
        }
        std::process::exit(1);
    }
}

fn run() -> Result<(), Error> {
    init_logger().expect("Failed to initialise logger.");
    let arg_matches = optparse::parse_cmd_options();
    settings::init(&arg_matches)?;

    let mut error_futures = Vec::new();

    let mut core = Core::new().unwrap();

    let broker_addr = SETTINGS.read().unwrap().get("broker_address")?;
    let broker_conn = broker::Amqp::connect(broker_addr, core.handle())?;
    error_futures.push(broker_conn.error_future);

    info!("Starting main event loop.");
    // The future that drives the loop is a select on all of the error futures of the background
    // services. As soon as one service fails, the event loop will terminate.
    core.run(future::select_all(error_futures))
        .map(|ok| ok.0)
        .map_err(|err| err.0)
}

fn init_logger() -> Result<(), Error> {
    let base_config = fern::Dispatch::new();

    let file_config = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Utc::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(fern::log_file("heracles-master.log")?);

    let stderr_config = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Utc::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stderr());

    base_config.chain(file_config).chain(stderr_config).apply()?;

    Ok(())
}
