//! Master program for the heracles network. Manages connections to all other services, accepts
//! inputs from the user clients, and splits up assigned work to pass to workers.

#![allow(unknown_lints)]
#![deny(missing_docs, missing_debug_implementations, missing_copy_implementations, trivial_casts,
        trivial_numeric_casts, unsafe_code, unstable_features, unused_import_braces,
        unused_qualifications)]

extern crate cerberus_proto;
extern crate chrono;
extern crate clap;
extern crate config;
extern crate failure;
extern crate fern;
extern crate futures;
extern crate lapin_futures as lapin;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate rayon;
extern crate tokio_core;
extern crate uuid;

mod broker;
mod optparse;
mod scheduler;
mod settings;
mod splitting;
mod state;

use failure::Error;
use futures::future;
use tokio_core::reactor::Core;

use broker::Broker;
use settings::SETTINGS;

const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

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
