#!/usr/bin/env sh

LOCATION=/tmp/heracles/demo

go run hrctl/hrctl.go schedule -f ${LOCATION}/count.pb
