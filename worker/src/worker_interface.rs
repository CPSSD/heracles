use std::net::SocketAddr;
use std::str::FromStr;
use std::path::Path;

use grpc::RequestOptions;

use errors::*;
use operations::io;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use cerberus_proto::worker_grpc::IntermediateDataService; // For pub functions only

/// `WorkerInterface` is used to load data from other workers which have completed
/// their map tasks.
pub struct WorkerInterface;

impl WorkerInterface {
    pub fn get_data<P: AsRef<Path>>(path: P, output_dir_uuid: &str) -> Result<String> {
        let path_str = path.as_ref().to_string_lossy();
        let split_path: Vec<&str> = path_str.splitn(2, '/').collect();
        let worker_addr =
            SocketAddr::from_str(split_path[0]).chain_err(|| "Unable to parse worker address")?;
        let file = format!("/{}", split_path[1]);
        info!("getting {} from {}", &file, worker_addr);

        if file.contains(output_dir_uuid) {
            info!("file {} is local, loading from disk", file);
            return io::read(file).chain_err(|| "Unable to read from local disk");
        }

        // TODO: Add client store so we don't need to create a new client every time.
        let client = grpc_pb::IntermediateDataServiceClient::new_plain(
            &worker_addr.ip().to_string(),
            worker_addr.port(),
            Default::default(),
        ).chain_err(|| format!("Error building client for worker {}", worker_addr))?;

        let mut req = pb::IntermediateDataRequest::new();
        req.set_path(file.clone());

        let res = client
            .get_intermediate_data(RequestOptions::new(), req)
            .wait()
            .chain_err(|| format!("Failed to get {} from {}", file, worker_addr))?
            .1;

        String::from_utf8(res.get_data().to_vec())
            .chain_err(|| "Unable to convert returned data to string")
    }
}
