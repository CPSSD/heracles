extern crate chrono;
extern crate failure;
extern crate fern;
extern crate heracles_manager;
#[macro_use]
extern crate log;
extern crate tokio;

use failure::*;
use tokio::prelude::*;

use heracles_manager::settings::SETTINGS;
use heracles_manager::{broker, optparse, scheduler, server, state, settings};

use std::path::PathBuf;
use std::sync::Arc;

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

    let broker_addr = SETTINGS.read().unwrap().get("broker.address")?;
    let broker_conn = broker::amqp::connect(broker_addr).wait()?;

    let state_location: &str = SETTINGS.read().unwrap().get("state.location")?;
    let store = state::FileStore::new(&PathBuf::from(state_location.to_string()))?;

    let schdlr = Arc::new(scheduler::Scheduler::new(Arc::new(broker_conn), Arc::new(store))?);

    server::Server::new(Arc::clone(&schdlr))?;

    info!("Starting main event loop.");
    // We give this an empty future so that it will never terminate and continue driving other
    // futures to completion.
    tokio::run(schdlr.run());
    // tokio::run(future::empty());
    Ok(())
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
