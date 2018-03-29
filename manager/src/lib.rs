//! Master library for the heracles network. Manages connections to all other services, accepts
//! inputs from the user clients, and splits up assigned work to pass to workers.

#![allow(unknown_lints)]
#![warn(missing_copy_implementations, trivial_casts, trivial_numeric_casts, unsafe_code,
        unused_import_braces, unused_qualifications)]
#![feature(conservative_impl_trait)]

extern crate chrono;
extern crate clap;
extern crate config;
extern crate failure;
extern crate futures;
extern crate grpc;
extern crate heracles_proto;
extern crate lapin_futures as lapin;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate rayon;
extern crate tokio;
extern crate uuid;

pub mod broker;
pub mod optparse;
pub mod scheduler;
pub mod server;
pub mod settings;
pub mod splitting;
pub mod state;

const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");
