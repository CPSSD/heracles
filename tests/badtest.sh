#!/usr/bin/env sh

docker kill rabbit
docker rm rabbit
docker run -d --hostname=rabbit --name=rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
sleep 10
go run ../tools/tester/tester.go
mkdir -p /tmp/heracles_test_jobs/jobs/test_job/tasks
mkdir -p /tmp/heracles_test_jobs/jobs/test_job/pending_map_tasks
mkdir -p /tmp/heracles_test_jobs/jobs/test_job/pending_reduce_tasks
mkdir -p /tmp/heracles_test_jobs/input
touch /tmp/heracles_test_jobs/jobs/test_job/tasks/test_task && echo "test" > /tmp/heracles_test_jobs/jobs/test_job/tasks/test_task
touch /tmp/heracles_test_jobs/jobs/test_job/pending_map_tasks/test_task && echo "test" > /tmp/heracles_test_jobs/jobs/test_job/pending_map_tasks/test_task
touch /tmp/heracles_test_jobs/jobs/test_job/pending_reduce_tasks/test_task && echo "test" > /tmp/heracles_test_jobs/jobs/test_job/pending_reduce_tasks/test_task
touch /tmp/heracles_test_jobs/input/wood && echo "how much wood could a wood chuck chuck if a wood chuck could chuck wood" > /tmp/heracles_test_jobs/input/wood
../target/debug/worker --logtostderr --broker.address=amqp://localhost:5672/ --v=2 --state.location=/tmp/heracles_test_jobs