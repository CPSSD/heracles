#!/usr/bin/env sh

LOCATION=/tmp/heracles/demo

docker run --rm -d --hostname=rabbit --name=rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
rm -rf $LOCATION/
mkdir -p $LOCATION/word-counter/input
mkdir -p $LOCATION/word-counter/out
mkdir -p $LOCATION/intermediate
mkdir -p $LOCATION/state
echo "sample1 sample2" > $LOCATION/word-counter/input/file1
echo "sample2 sample3" > $LOCATION/word-counter/input/file2
cp ./target/debug/examples/word-counter $LOCATION/word-counter/

tee $LOCATION/count.pb <<PB
input_directory: "${LOCATION}/word-counter/input"
payload_path: "${LOCATION}/word-counter/word-counter"
input_kind: DATA_TEXT_NEWLINES
output_files: "${LOCATION}/word-counter/out/output"
PB