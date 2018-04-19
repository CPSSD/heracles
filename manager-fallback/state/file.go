package state

import (
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/cpssd/heracles/manager-fallback/settings"
	"github.com/cpssd/heracles/proto/datatypes"
	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

const (
	jobsDir          = "jobs"
	jobSaveFile      = "request"
	tasksDir         = "tasks"
	pendingMapDir    = "pending_map_tasks"
	pendingReduceDir = "pending_reduce_tasks"
)

// FileStore implementation
type FileStore struct {
	location string
}

// NewFileStore returns a new file backed state store
func NewFileStore(location string) (*FileStore, error) {
	// TODO: remove this from here. It is not really state.
	if err := os.MkdirAll(settings.String("scheduler.intermediate_data_location"), os.ModePerm); err != nil {
		return nil, err
	}

	// TODO: Should not be 0777
	if err := os.MkdirAll(path.Join(location, jobsDir), os.ModePerm); err != nil {
		return nil, err
	}

	log.Info("Using file backed storage")
	return &FileStore{location}, nil
}

// SaveJob implementation
func (f FileStore) SaveJob(job *datatypes.Job) error {
	log.V(2).Infof("Saving job %s", job.GetId())
	id := job.GetId()

	jobDirPath := path.Join(f.location, jobsDir, id)
	if err := f.prepareJobDirectory(jobDirPath); err != nil {
		log.Error(err)
		f.removeJob(id)
		return err
	}

	serialized, err := proto.Marshal(job)
	if err != nil {
		f.removeJob(id)
		return err
	}

	if err := ioutil.WriteFile(path.Join(jobDirPath, jobSaveFile), serialized, 0600); err != nil {
		f.removeJob(id)
		return err
	}

	log.V(1).Infof("Successfully saved job %s", job.GetId())

	return nil
}

// SaveTask implementation
func (f FileStore) SaveTask(task *datatypes.Task) error {
	jobDirPath := path.Join(f.location, jobsDir, task.GetJobId())
	id := task.GetId()

	taskFilePath := path.Join(jobDirPath, tasksDir, id)
	if _, err := os.Stat(taskFilePath); os.IsNotExist(err) {
		return err
	}

	var pendingFilePath string

	switch task.GetKind() {
	case datatypes.TaskKind_MAP:
		pendingFilePath = path.Join(jobDirPath, pendingMapDir, id)
	case datatypes.TaskKind_REDUCE:
		pendingFilePath = path.Join(jobDirPath, pendingReduceDir, id)
	}

	if _, err := os.Stat(pendingFilePath); os.IsNotExist(err) {
		return err
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

// CreateTasks implementation
func (f FileStore) CreateTasks(tasks []*datatypes.Task) error {
	for _, task := range tasks {
		if err := f.createTask(task); err != nil {
			return err
		}
	}
	return nil
}

// WaitUntilTasksComplete implementation
func (f FileStore) WaitUntilTasksComplete(id string, kind datatypes.TaskKind) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	pendingDirPath := path.Join(f.location, jobsDir, id)
	switch kind {
	case datatypes.TaskKind_MAP:
		pendingDirPath = path.Join(pendingDirPath, pendingMapDir)
	case datatypes.TaskKind_REDUCE:
		pendingDirPath = path.Join(pendingDirPath, pendingReduceDir)
	}

	log.V(1).Infof("watching %s for changes", pendingDirPath)

	done := make(chan struct{})
	go func() {
		// TODO: make this configurarable through settings
		timer := time.NewTimer(2 * time.Second)

		for {
			select {
			case <-watcher.Events:
				files, err := ioutil.ReadDir(pendingDirPath)
				if err != nil {
					log.Error(err)
				}
				if len(files) == 0 {
					close(done)
					return
				}
			case <-timer.C:
				files, err := ioutil.ReadDir(pendingDirPath)
				if err != nil {
					log.Error(err)
				}
				if len(files) == 0 {
					close(done)
					return
				}
			case err := <-watcher.Errors:
				log.Error(err)
			}
		}
	}()

	if err := watcher.Add(pendingDirPath); err != nil {
		log.Error("unable to add watcher")
		return err
	}

	<-done
	return nil
}

func (f FileStore) createTask(task *datatypes.Task) error {
	jobDirPath := path.Join(f.location, jobsDir, task.GetJobId())
	id := task.GetId()

	taskFilePath := path.Join(jobDirPath, tasksDir, id)
	if _, err := os.Create(taskFilePath); err != nil {
		return err
	}

	var pendingFilePath string
	switch task.GetKind() {
	case datatypes.TaskKind_MAP:
		pendingFilePath = path.Join(jobDirPath, pendingMapDir, id)
	case datatypes.TaskKind_REDUCE:
		pendingFilePath = path.Join(jobDirPath, pendingReduceDir, id)
	}
	if _, err := os.Create(pendingFilePath); err != nil {
		return err
	}
	return nil
}

// PendingMapTasks implementation
func (f FileStore) PendingMapTasks(*datatypes.Job) ([]*datatypes.Task, error) {
	return nil, nil
}

// PendingReduceTasks implementation
func (f FileStore) PendingReduceTasks(*datatypes.Job) ([]*datatypes.Task, error) {
	return nil, nil
}

func (f FileStore) removeJob(id string) {
	os.RemoveAll(path.Join(f.location, jobsDir, id))
}

func (f FileStore) prepareJobDirectory(loc string) error {
	if err := os.MkdirAll(path.Join(loc, tasksDir), os.ModePerm); err != nil {
		return err
	}
	log.V(2).Info("created tasks dir")
	if err := os.MkdirAll(path.Join(loc, pendingMapDir), os.ModePerm); err != nil {
		return err
	}
	log.V(2).Info("created pending map dir")
	if err := os.MkdirAll(path.Join(loc, pendingReduceDir), os.ModePerm); err != nil {
		return err
	}
	log.V(2).Info("created pending reduce dir")
	return nil
}
