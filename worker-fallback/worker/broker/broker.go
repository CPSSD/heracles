package broker

import (
	"github.com/cpssd/heracles/proto/datatypes"
	"github.com/cpssd/heracles/worker-fallback/worker/settings"
)

// Broker interface contains the functions needed to receive data
type Broker interface {
	// Listen to incoming tasks from broker
	Listen() error
	// Tasks returns the channel on which the tasks will be sent.
	Tasks() (<-chan *datatypes.Task, error)

	// Done marks which messages are done for and can be acknowledged
	Done(*datatypes.Task) error

	// Failed marks a message to a broker than it has failed.
	Failed(*datatypes.Task) error
}

// New returns a new broker based on the settings
func New() (Broker, error) {
	addr := settings.GetString("broker.address")
	return NewAMQP(addr)
}
