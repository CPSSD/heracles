package broker

import (
	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/cpssd/heracles/proto/datatypes"
	"github.com/cpssd/heracles/worker/settings"
)

// AMQP Errors
var (
	ErrUnknownTag = errors.New("unknown tag")
	ErrAckFailure = errors.New("failed to ack message")
)

// AMQPConnection implements broker
type AMQPConnection struct {
	conn      *amqp.Connection
	ch        channel
	tags      map[string]*amqp.Delivery
	tasks     chan *datatypes.Task
	queueName string
}

// channel is an abstraction over amqp.Channel to allow for easier testing.
// Only the functions needed are implemented
type channel interface {
	Ack(uint64, bool) error
	Nack(uint64, bool, bool) error
	Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)
}

// NewAMQP creates a new connection to AMQP
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

	queueName := settings.GetString("broker.queue_name")

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
		tags:      make(map[string]*amqp.Delivery),
		queueName: queueName,
	}, nil
}

// Done implementation
func (c *AMQPConnection) Done(task *datatypes.Task) error {
	if d, ok := c.tags[task.GetId()]; ok {
		if err := d.Ack(false); err != nil {
			log.Warningf("can't ack task %s: %v", task.GetId(), err)
			return ErrAckFailure
		}
		return nil
	}
	return ErrUnknownTag
}

// Failed implementation
func (c *AMQPConnection) Failed(task *datatypes.Task) error {
	if d, ok := c.tags[task.GetId()]; ok {
		if err := d.Nack(false, true); err != nil {
			log.Warningf("can't nack task %s: %v", task.GetId(), err)
			return ErrAckFailure
		}
		return nil
	}
	return ErrUnknownTag
}

// Listen to messages. Should be started as a goroutine
func (c *AMQPConnection) Listen() error {
	c.tasks = make(chan *datatypes.Task)

	msgs, err := c.ch.Consume(
		c.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "unable to consume")
	}

	for msg := range msgs {
		task := &datatypes.Task{}
		if err := proto.Unmarshal(msg.Body, task); err != nil {
			return errors.Wrap(err, "unable to parse task")
		}
		log.V(2).Infof("got task %s with tag %d", task.GetId(), msg.DeliveryTag)
		c.tags[task.GetId()] = &msg
		c.tasks <- task
	}

	return nil
}

// Tasks returns a channel on which the messages will be received.
func (c *AMQPConnection) Tasks() (<-chan *datatypes.Task, error) {
	if c.tasks == nil {
		return nil, errors.New("the broker is not listening")
	}
	return c.tasks, nil
}

// Channel returns the open channel of the broker. This this method
// should not be used directly and is for debugging only.
func (c *AMQPConnection) Channel() *amqp.Channel {
	return c.ch.(*amqp.Channel)
}
