all: build

deps:
	go get -v github.com/golang/protobuf/proto
	go get -v github.com/golang/protobuf/protoc-gen-go

build: deps
	mkdir -p datatypes mapreduce
	protoc datatypes.proto --go_out=datatypes
	protoc mapreduce.proto --go_out=plugins=grpc,Mdatatypes.proto=github.com/cpssd/heracles/proto/datatypes:mapreduce