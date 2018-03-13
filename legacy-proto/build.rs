extern crate protoc_rust_grpc;

fn main() {
    protoc_rust_grpc::run(protoc_rust_grpc::Args {
        out_dir: "src",
        includes: &["."],
        input: &["mapreduce.proto", "worker.proto"],
        rust_protobuf: true, // also generate protobuf messages, not just services
    }).expect("protoc-rust-grpc");
}
