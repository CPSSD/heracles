//! Master program for the heracles network. Manages connections to all other services, accepts
//! inputs from the user clients, and splits up assigned work to pass to workers.

#![allow(unknown_lints)]
#![deny(missing_docs, missing_debug_implementations, missing_copy_implementations, trivial_casts,
        trivial_numeric_casts, unsafe_code, unstable_features, unused_import_braces,
        unused_qualifications)]

extern crate cerberus_proto;
extern crate chrono;
extern crate clap;
extern crate failure;
extern crate fern;
extern crate futures;
extern crate lapin_futures as lapin;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate tokio_core;

mod broker;

use failure::Error;
use futures::future;
use tokio_core::reactor::Core;

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
    init_logger()?;

    let mut core = Core::new().unwrap();

    // We don't have a main future that needs executing, but we need something to drive the event
    // loop. An empty future will always return `Async::NotReady` and will thus drive the loop.
    core.run(future::empty::<(), ()>()).unwrap();

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
