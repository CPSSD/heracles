package state

import (
	"github.com/cpssd/heracles/proto/datatypes"
	log "github.com/golang/glog"
)

// EmptyStore implements State
type EmptyStore struct {
}

// NewEmptyStore returns a state store which does nothing.
func NewEmptyStore() (*EmptyStore, error) {
	return &EmptyStore{}, nil
}

// SaveProgress implementation
func (f EmptyStore) SaveProgress(task *datatypes.Task) error {
	log.V(1).Info("empty store saving task %s", task.GetId())
	return nil
}
