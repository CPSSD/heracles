use std::net::SocketAddr;

use cerberus_proto::datatypes::Job;
use super::*;

pub struct EtcdStore;

impl State for EtcdStore {
    fn connect(addrs: Vec<SocketAddr>, handle: Handle) -> Result<(), StateError> {
        unimplemented!()
    }

    fn error_future() -> Box<Future<Item = (), Error = StateError>> {
        unimplemented!()
    }

    fn save_job(job: &Job) -> Result<(), StateError> {
        unimplemented!()
    }
}
