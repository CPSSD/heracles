//! Worker program for the heracles network. Performs the maps and reduces.

#![allow(unknown_lints)]
#![cfg_attr(test, feature(proc_macro))]
#![deny(missing_docs, missing_debug_implementations, missing_copy_implementations, trivial_casts,
        trivial_numeric_casts, unsafe_code, unused_import_braces, unused_qualifications)]
#![feature(conservative_impl_trait)]

extern crate bson;
extern crate cerberus_proto;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;
extern crate futures;
extern crate grpc;
extern crate libc;
extern crate local_ip;
#[macro_use]
extern crate log;
extern crate procinfo;
extern crate protobuf;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tls_api;
extern crate util;
extern crate uuid;

#[cfg(test)]
extern crate mocktopus;

mod master_interface;
mod operations;
mod server;
mod parser;
mod worker_interface;

use std::{thread, time};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use failure::*;

use master_interface::MasterInterface;
use operations::OperationHandler;
use server::{IntermediateDataService, ScheduleOperationService, Server};
use util::init_logger;

const WORKER_REGISTRATION_RETRIES: u16 = 5;
const MAX_HEALTH_CHECK_FAILURES: u16 = 10;
const MAIN_LOOP_SLEEP_MS: u64 = 3000;
const WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS: u64 = 1000;
// Setting the port to 0 means a random available port will be selected
const DEFAULT_PORT: &str = "0";
const DEFAULT_MASTER_ADDR: &str = "[::]:8081";

fn register_worker(master_interface: &MasterInterface, address: &SocketAddr) -> Result<(), Error> {
    let mut retries = WORKER_REGISTRATION_RETRIES;
    while retries > 0 {
        retries -= 1;

        match master_interface.register_worker(address) {
            Ok(_) => break,
            Err(err) => {
                if retries == 0 {
                    return Err(err.chain_err(|| "Error registering worker with master"));
                }
            }
        }

        thread::sleep(time::Duration::from_millis(
            WORKER_REGISTRATION_RETRY_WAIT_DURATION_MS,
        ));
    }

    Ok(())
}

fn run() -> Result<(), Error> {
    println!("Cerberus Worker!");
    init_logger().chain_err(|| "Failed to initialise logging.")?;

    let matches = parser::parse_command_line();
    let master_addr = SocketAddr::from_str(
        matches.value_of("master").unwrap_or(DEFAULT_MASTER_ADDR),
    ).chain_err(|| "Error parsing master address")?;
    let port = u16::from_str(matches.value_of("port").unwrap_or(DEFAULT_PORT))
        .chain_err(|| "Error parsing port")?;

    let master_interface = Arc::new(MasterInterface::new(master_addr).chain_err(|| "Error creating master interface.")?);
    let operation_handler = Arc::new(OperationHandler::new(Arc::clone(&master_interface)));

    let scheduler_service = ScheduleOperationService::new(Arc::clone(&operation_handler));
    let interm_data_service = IntermediateDataService;
    let srv = Server::new(port, scheduler_service, interm_data_service)
        .chain_err(|| "Can't create server")?;

    let local_addr = SocketAddr::from_str(&format!(
        "{}:{}",
        local_ip::get().expect("Could not get IP"),
        srv.addr().port(),
    )).chain_err(|| "Not a valid address of the worker")?;
    register_worker(&*master_interface, &local_addr).chain_err(|| "Failed to register worker.")?;

    info!(
        "Successfully registered worker ({}) with master on {}",
        local_addr.to_string(),
        master_addr.to_string(),
    );

    let mut current_health_check_failures = 0;

    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_SLEEP_MS));

        if let Err(err) = operation_handler.update_worker_status() {
            error!("Could not send updated worker status to master: {}", err);
            current_health_check_failures += 1;
        } else {
            current_health_check_failures = 0;
        }

        if current_health_check_failures >= MAX_HEALTH_CHECK_FAILURES {
            if let Err(err) = register_worker(&*master_interface, &local_addr) {
                error!("Failed to re-register worker after disconnecting: {}", err);
            } else {
                info!("Successfully re-registered with master after being disconnected.");
                current_health_check_failures = 0;
            }
        }

        if !srv.is_alive() {
            return Err("Worker interface server has unexpectingly died.".into());
        }
    }
}

fn main() {
    if let Err(err) = run() {
        error!("{}", err);
        for cause in err.causes().skip(1) {
            error!("Caused by: {}", cause);
        }
        std::process::exit(1);
    }
}
