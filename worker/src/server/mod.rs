/// `master_service` is responsible for handing data incoming from the master.
mod master_service;

pub use self::master_service::ScheduleOperationService;

use std::net::SocketAddr;
use grpc;
use cerberus_proto::worker_grpc;
use errors::*;

const GRPC_THREAD_POOL_SIZE: usize = 1;

pub struct Server {
    server: grpc::Server,
}

impl Server {
    pub fn new(port: u16, scheduler_service: ScheduleOperationService) -> Result<Self> {
        let mut server_builder = grpc::ServerBuilder::new_plain();
        server_builder.http.set_port(port);
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        // Register the ScheduleOperationService
        server_builder.add_service(
            worker_grpc::ScheduleOperationServiceServer::new_service_def(
                scheduler_service,
            ),
        );

        Ok(Server {
            server: server_builder.build().chain_err(
                || "Error building gRPC server",
            )?,
        })
    }

    pub fn is_alive(&self) -> bool {
        self.server.is_alive()
    }

    pub fn addr(&self) -> &SocketAddr {
        self.server.local_addr()
    }
}