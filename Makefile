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

go-proto:
	cd proto && make

go-build: go-proto
	go build -o ./target/debug/manager-fallback manager-fallback/manager.go
	go build -o ./target/debug/worker worker/worker.go
	go build -o ./target/debug/hrctl hrctl/hrctl.go
