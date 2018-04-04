package broker

import (
	"reflect"
	"testing"

	"github.com/cpssd/heracles/proto/datatypes"
	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

type MockChannel struct {
	delivery  chan amqp.Delivery
	acksError error
}

func (ch MockChannel) Ack(tag uint64, _ bool) error {
	return ch.acksError
}

func (ch MockChannel) Nack(tag uint64, _, _ bool) error {
	return ch.acksError
}

func (ch MockChannel) Consume(_, _ string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	return ch.delivery, nil
}

func TestListen(t *testing.T) {

	ch := &MockChannel{
		delivery: make(chan amqp.Delivery),
	}

	c := &AMQPConnection{
		ch:   ch,
		tags: make(map[string]*amqp.Delivery),
	}

	waitTillStarted := make(chan bool)
	go func() {
		close(waitTillStarted)
		if err := c.Listen(); err != nil {
			t.Errorf("unexpected error when listening: %v", err)
			return
		}
	}()
	<-waitTillStarted

	task := &datatypes.Task{
		Id: "broker_test",
	}

	waitTillTaskReceived := make(chan bool)
	go func() {
		defer close(waitTillTaskReceived)
		receivedTask := <-c.tasks
		if !reflect.DeepEqual(task, receivedTask) {
			t.Errorf("both tasks not closed")
			return
		}
	}()

	marshalledTask, err := proto.Marshal(task)
	if err != nil {
		t.Errorf("unable to marshal task: %v", err)
		return
	}

	ch.delivery <- amqp.Delivery{
		DeliveryTag: 1,
		Body:        marshalledTask,
	}

	<-waitTillTaskReceived
	close(ch.delivery)

	// if tag, ok := c.tags["broker_test"]; tag != 1 || !ok {
	// 	t.Errorf("invalid or missing tag. Wanted %d, got %d", 1, tag)
	// }
}

func TestTasks(t *testing.T) {
	c := &AMQPConnection{}
	if _, err := c.Tasks(); err == nil {
		t.Error("expected an error, have nil")
		return
	}

	c.tasks = make(chan *datatypes.Task)

	if _, err := c.Tasks(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// func TestDoneFailed(t *testing.T) {
// 	testCases := []struct {
// 		taskID      string
// 		tag         uint64
// 		ack         bool // false for nack
// 		returnError error
// 	}{
// 		{
// 			taskID:      "bad",
// 			tag:         1,
// 			ack:         true,
// 			returnError: ErrUnknownTag,
// 		}, {
// 			taskID:      "ack_1",
// 			tag:         2,
// 			ack:         true,
// 			returnError: ErrAckFailure,
// 		}, {
// 			taskID:      "ack_2",
// 			tag:         3,
// 			ack:         true,
// 			returnError: nil,
// 		}, {
// 			taskID:      "bad",
// 			tag:         4,
// 			ack:         false,
// 			returnError: ErrUnknownTag,
// 		}, {
// 			taskID:      "nack_1",
// 			tag:         5,
// 			ack:         false,
// 			returnError: ErrAckFailure,
// 		}, {
// 			taskID:      "nack_2",
// 			tag:         6,
// 			ack:         false,
// 			returnError: nil,
// 		},
// 	}

// 	ch := &MockChannel{}

// 	c := &AMQPConnection{
// 		ch: ch,
// 		tags: map[string]uint64{
// 			"ack_1":  2,
// 			"ack_2":  3,
// 			"nack_1": 5,
// 			"nack_2": 6,
// 		},
// 	}

// 	for _, test := range testCases {
// 		t.Logf("%+v", test)

// 		ch.acksError = test.returnError
// 		var err error
// 		if test.ack {
// 			err = c.Done(&datatypes.Task{Id: test.taskID})
// 		} else {
// 			err = c.Failed(&datatypes.Task{Id: test.taskID})
// 		}

// 		t.Log(err)
// 		t.Log(test.returnError)
// 		continue

// 		if err.Error() != test.returnError.Error() {
// 			t.Errorf("was expecting %v, got %v", test.returnError, err)
// 			return
// 		}
// 	}

// 	if err := c.Done(&datatypes.Task{Id: "bad_id"}); err == nil {
// 		t.Errorf("expected an error for missing tag, got nil")
// 	}

// 	if err := c.Done(&datatypes.Task{Id: "ack_test"}); err == nil {
// 		t.Errorf("expected an error for failing to ack")
// 	}
// }
