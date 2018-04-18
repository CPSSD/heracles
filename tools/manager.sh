#!/usr/bin/env sh

LOCATION=/tmp/heracles/demo

go run manager-fallback/manager.go \
    --logtostderr \
    -v=2 \
    --broker.address=amqp://localhost:5672/ \
    --state.location=${LOCATION}/state \
    --scheduler.intermediate_data_location=${LOCATION}/intermediate \
    |& pp
