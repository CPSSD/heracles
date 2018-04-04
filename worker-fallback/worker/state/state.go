package state

import (
	"github.com/cpssd/heracles/proto/datatypes"
	"github.com/cpssd/heracles/worker-fallback/worker/settings"
	"github.com/pkg/errors"
)

// State contains the availabel functions for worker
type State interface {
	// Save a task in the state
	SaveProgress(*datatypes.Task) error
}

// New returns a new state store
func New() (State, error) {
	kind := settings.Get("state.backend").(string)
	switch kind {
	case "file":
		location := settings.Get("state.location").(string)
		return NewFileStore(location)
	}

	return nil, errors.New("unknown state kind")
}
