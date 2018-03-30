all:
	mkdir -p datatypes mapreduce
	protoc datatypes.proto --go_out=datatypes
	protoc mapreduce.proto --go_out=plugins=grpc,Mdatatypes.proto=github.com/cpssd/heracles/proto/datatypes:mapreduce