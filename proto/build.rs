extern crate protoc_rust_grpc;

use std::process::Command;
use std::fs;

fn main() {
    protoc_rust_grpc::run(protoc_rust_grpc::Args {
        out_dir: "src",
        includes: &["."],
        input: &["datatypes.proto", "mapreduce.proto"],
        rust_protobuf: true, // also generate protobuf messages, not just services
    }).expect("protoc-rust-grpc")
}
