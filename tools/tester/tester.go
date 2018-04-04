package main

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"

	"github.com/cpssd/heracles/proto/datatypes"
	"github.com/cpssd/heracles/worker/broker"
	"github.com/cpssd/heracles/worker/settings"
)

func main() {
	settings.Init()

	c, _ := broker.NewAMQP("amqp://localhost:5672/")
	ch := c.Channel()

	var task = &datatypes.Task{
		Id:          "test_task",
		JobId:       "test_job",
		PayloadPath: "../target/debug/examples/word-counter",
		InputChunk: &datatypes.InputChunk{
			Path: "/tmp/heracles_test_jobs/input/wood",
		},
		OutputFiles:    []string{"wood", "chuck"},
		PartitionCount: 2,
	}

	seralized, _ := proto.Marshal(task)

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         seralized,
	}

	if err := ch.Publish(
		"",
		settings.GetString("broker.queue_name"),
		false,
		false,
		msg,
	); err != nil {
		panic(err)
	}
}
