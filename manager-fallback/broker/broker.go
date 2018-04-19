package broker

import (
	"github.com/cpssd/heracles/manager-fallback/settings"
	"github.com/cpssd/heracles/proto/datatypes"
)

// Broker interface
type Broker interface {
	Send(*datatypes.Task) error
}

// New broker connection
func New() (Broker, error) {
	addr := settings.GetString("broker.address")
	return NewAMQP(addr)
}
