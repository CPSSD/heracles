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

    let mut core = Core::new().unwrap();

    let broker_addr = SETTINGS.read().unwrap().get("broker_address")?;
    let broker_channel = broker::amqp::connect(broker_addr, core.handle());

    info!("Starting main event loop.");
    // We give this an empty future so that it will never terminate and continue driving other
    // futures to completion.
    core.run(future::empty())
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
