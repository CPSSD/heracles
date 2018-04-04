package state

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/cpssd/heracles/proto/datatypes"
)

func setup(testPath string, taskID string) {
	tasksDirPath := path.Join(testPath, tasksDir)
	os.MkdirAll(tasksDirPath, 0777)
	ioutil.WriteFile(
		path.Join(tasksDirPath, taskID),
		[]byte(taskID),
		0644,
	)

	pendingTasksDirPath := path.Join(testPath, pendingMapDir)
	os.MkdirAll(pendingTasksDirPath, 0777)
	ioutil.WriteFile(
		path.Join(pendingTasksDirPath, taskID),
		[]byte(taskID),
		0644,
	)
}

func cleanup(testPath string) {
	os.RemoveAll(testPath)
}

func TestFileStore(t *testing.T) {
	testDir, err := ioutil.TempDir("", "heracles_worker_state_test")
	if err != nil {
		t.Errorf("unable to create test directory: %v", err)
	}

	st, err := NewFileStore(testDir)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	taskID := "test_task"
	jobID := "test_job"

	testJobPath := path.Join(testDir, jobsDir, jobID)
	setup(testJobPath, taskID)
	defer cleanup(testDir)

	testTask := &datatypes.Task{
		Id:     taskID,
		JobId:  jobID,
		Kind:   datatypes.TaskKind_MAP,
		Status: datatypes.TaskStatus_TASK_IN_PROGRESS,
	}
	if err := st.SaveProgress(testTask); err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	pendingTasksDirPath := path.Join(testJobPath, pendingMapDir, taskID)

	if _, err := os.Stat(pendingTasksDirPath); os.IsNotExist(err) {
		t.Error("pending map file should exist and doesnt't")
	}

	testTask.Status = datatypes.TaskStatus_TASK_DONE
	if err := st.SaveProgress(testTask); err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if _, err := os.Stat(pendingTasksDirPath); !os.IsNotExist(err) {
		t.Error("pending map file should not exist")
	}
}
