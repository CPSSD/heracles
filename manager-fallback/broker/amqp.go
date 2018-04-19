package broker

import (
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/cpssd/heracles/manager-fallback/settings"
	"github.com/cpssd/heracles/proto/datatypes"
)

// AMQPConnection broker
type AMQPConnection struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
}

type channel interface {
	Publish()
}

// NewAMQP returns a new connection to AMQP
func NewAMQP(addr string) (*AMQPConnection, error) {
	log.Infof("connecting to broker at %s", addr)
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open connection to AMQP")
	}
	// TODO: Handle closing of connection

	log.Info("connected to broker")

	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a channel")
	}

	queueName := settings.String("broker.queue_name")

	if _, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, errors.Wrap(err, "unable to declare a queue")
	}

	return &AMQPConnection{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
	}, nil
}

// Send implementation. Call this in go routine. This function will only return
// When positively acknowledged. It can also return an error if something happens
// beforehand
func (c *AMQPConnection) Send(task *datatypes.Task) error {
	serialized, err := proto.Marshal(task)
	if err != nil {
		return errors.Wrap(err, "can't serialize")
	}

	if err := c.ch.Publish(
		"",
		settings.String("broker.queue_name"),
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         serialized,
		},
	); err != nil {
		return err
	}

	log.V(1).Infof("Sent task %s", task.GetId())

	return nil
}
