all: build

deps:
	go get -v github.com/golang/protobuf/proto
	go get -v github.com/golang/protobuf/protoc-gen-go

build:
	@echo -e "\n\n\tThis target has been deprecated. Use root's go-proto\n\n"
	cd .. && make go-proto
	# protoc mapreduce.proto --go_out=plugins=grpc