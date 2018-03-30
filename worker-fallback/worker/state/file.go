package state

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/pkg/errors"

	"github.com/cpssd/heracles/proto/datatypes"
	"github.com/golang/protobuf/proto"

	log "github.com/golang/glog"
)

const (
	jobsDir          = "jobs"
	tasksDir         = "tasks"
	pendingMapDir    = "pending_map_tasks"
	pendingReduceDir = "pending_reduce_tasks"
)

// FileStore implements State
type FileStore struct {
	path string
}

// NewFileStore creates a new file backed state
func NewFileStore(loc string) (*FileStore, error) {
	return &FileStore{
		path: loc,
	}, nil
}

// SaveProgress implementation
func (f FileStore) SaveProgress(task *datatypes.Task) error {
	jobDirPath := path.Join(f.path, jobsDir, task.GetJobId())
	id := task.GetId()

	taskFilePath := path.Join(jobDirPath, tasksDir, id)
	if _, err := os.Stat(taskFilePath); os.IsNotExist(err) {
		return errors.Wrap(err, "missing task")
	}

	var pendingFilePath string

	switch task.GetKind() {
	case datatypes.TaskKind_MAP:
		pendingFilePath = path.Join(jobDirPath, pendingMapDir, id)
	case datatypes.TaskKind_REDUCE:
		pendingFilePath = path.Join(jobDirPath, pendingReduceDir, id)
	}

	if _, err := os.Stat(pendingFilePath); os.IsNotExist(err) {
		return errors.Wrap(err, "missing pending file")
	}

	serializedTask, err := proto.Marshal(task)
	if err != nil {
		return errors.Wrap(err, "unable to serialize task")
	}

	if err := ioutil.WriteFile(taskFilePath, serializedTask, 0644); err != nil {
		return errors.Wrapf(err, "unable to write task %s", task.GetId())
	}

	if task.GetStatus() == datatypes.TaskStatus_TASK_DONE {
		log.V(1).Infof("removing task %s because its done", task.GetId())
		if err := os.Remove(pendingFilePath); err != nil {
			return errors.Wrapf(err, "unable to remove pending task %s", task.GetId())
		}
	}

	log.V(1).Infof("successfully saved task %s", task.GetId())

	return nil
}
