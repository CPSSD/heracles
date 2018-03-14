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

go-build: cli2 worker-fallback

go-deps:
	go get -v github.com/golang/protobuf/proto
	go get -v github.com/golang/protobuf/protoc-gen-go
	-go get -v -u ./worker-fallback/...
	-go get -v -u ./cmd/hrctl/...

go-proto: go-deps
	cd proto && make

worker-fallback: go-proto
	cd worker-fallback && make

cli2: go-proto
	cd cmd && make hrctl