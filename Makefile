all: build

.PHONY: build release test build-docker-images clean clean-all worker-fallback

# Bulds the debug version of cerberus
build:
	cargo build --verbose --all

# Create the release version
release:
	cargo build --verbose --all --release

clean:
	cargo clean

#############################################################

# Runs all the tests
test: unit-test

unit-test:
	cargo test --verbose --all

integration-test:
	./tests/integration.sh

#############################################################

build-docker-images: release
	docker build -t cpssd/cerberus-master -f master/Dockerfile .
	docker build -t cpssd/cerberus-worker -f worker/Dockerfile .


docker-compose-up: clean-docker-images build-docker-images
	docker-compose -f examples/cloud/cerberus-docker/docker-compose.yml -p cerberus up -d --scale worker=5 --force-recreate

docker-compose-down:
	docker-compose -f examples/cloud/cerberus-docker/docker-compose.yml -p cerberus down

clean-docker-images:
	docker rmi cpssd/cerberus-master -f
	docker rmi cpssd/cerberus-worker -f

clean-docker: docker-compose-down clean-docker-images

clean-all: clean-docker clean

#############################################################

go-proto-deps:
	go get -v github.com/golang/protobuf/proto
	go get -v github.com/golang/protobuf/protoc-gen-go

go-proto: go-proto-deps
	@mkdir -p proto/datatypes proto/mapreduce
	protoc proto/datatypes.proto --go_out=proto/datatypes

	protoc proto/mapreduce.proto --go_out=plugins=grpc,Mproto/datatypes.proto=github.com/cpssd/heracles/proto/datatypes:proto/mapreduce

	# This is a weird issue where the files are generated not where they are
	# supposed to be.
	mv proto/datatypes/proto/datatypes.pb.go proto/datatypes
	mv proto/mapreduce/proto/mapreduce.pb.go proto/mapreduce
	rmdir proto/datatypes/proto proto/mapreduce/proto

go-build: go-proto
	go build -o ./target/debug/manager-fallback manager-fallback/manager.go
	go build -o ./target/debug/worker worker/worker.go
	go build -o ./target/debug/hrctl hrctl/hrctl.go
