#!/usr/bin/env sh

LOCATION=/tmp/heracles/demo

go run worker/worker.go \
    --logtostderr \
    --broker.address=amqp://localhost:5672/ \
    --v=2 \
    --state.location="${LOCATION}/state" \
    |& pp