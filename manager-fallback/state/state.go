package state

import (
	"github.com/pkg/errors"

	"github.com/cpssd/heracles/manager-fallback/settings"
	"github.com/cpssd/heracles/proto/datatypes"
)

// State defintion
type State interface {
	SaveJob(*datatypes.Job) error
	SaveTask(*datatypes.Task) error
	CreateTasks([]*datatypes.Task) error
	WaitUntilTasksComplete(string, datatypes.TaskKind) error
	// PendingMapTasks(*datatypes.Job) ([]*datatypes.Task, error)
	// PendingReduceTasks(*datatypes.Job) ([]*datatypes.Task, error)
}

// New returns state
func New() (State, error) {
	switch settings.String("state.backend") {
	case "file":
		return NewFileStore(settings.String("state.location"))
	}

	return nil, errors.New("unknown state kind")
}
