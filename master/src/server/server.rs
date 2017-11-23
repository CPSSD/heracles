use errors::*;
use grpc;

use server::client_service::ClientService;
use server::worker_service::WorkerService;

use cerberus_proto::{mapreduce_grpc, worker_grpc};

const GRPC_THREAD_POOL_SIZE: usize = 1;

pub struct Server {
    server: grpc::Server,
}

impl Server {
    pub fn new(
        port: u16,
        client_service: ClientService,
        worker_service: WorkerService,
    ) -> Result<Self> {
        let mut server_builder = grpc::ServerBuilder::new_plain();
        server_builder.http.set_port(port);
        server_builder.http.set_cpu_pool_threads(
            GRPC_THREAD_POOL_SIZE,
        );

        // Register the MapReduceService
        server_builder.add_service(mapreduce_grpc::MapReduceServiceServer::new_service_def(
            client_service,
        ));

        // Register the WorkerServiceServer
        server_builder.add_service(worker_grpc::WorkerServiceServer::new_service_def(
            worker_service,
        ));

        Ok(Server {
            server: server_builder.build().chain_err(
                || "Error building grpc server",
            )?,
        })
    }

    pub fn is_alive(&self) -> bool {
        self.server.is_alive()
    }
}

#[cfg(test)]
mod tests {}