// Package runner runs the tasks
// TODO: Probably rename to handler
package runner

import (
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cpssd/heracles/proto/datatypes"
	"github.com/cpssd/heracles/worker-fallback/worker/broker"
	"github.com/cpssd/heracles/worker-fallback/worker/state"

	log "github.com/golang/glog"
)

// Runner object which listens for broker connections, runs the
// task and saves the state to state store.
type Runner struct {
	st state.State
	br broker.Broker
}

// New create a new runner
func New(st state.State, br broker.Broker) *Runner {
	return &Runner{
		st: st,
		br: br,
	}
}

// Run the individual tasks
func (r Runner) Run() error {
	// make sure the broker is already listening
	taskChan, err := r.br.Tasks()
	if err != nil {
		return errors.Wrap(err, "unable to recieve tasks")
	}

	log.V(1).Info("listening to incoming tasks")

	var wg sync.WaitGroup
	for task := range taskChan {
		wg.Add(1)
		log.Infof("got new task %s", task.GetId())
		go func(task *datatypes.Task) {
			defer wg.Done()

			if err := r.handleTask(task); err != nil {
				r.failTask(task)
				log.Warningf("unable to run task: %v", err)
				return
			}
			if err := r.succeedTask(task); err != nil {
				r.failTask(task)
				log.Warningf("unable to succeed task: %v", err)
			}
		}(task)
	}

	log.V(1).Info("waiting for all tasks to finish")
	wg.Wait()
	return nil
}

// handleTask takes in a task, actually runs the payload and saves the data to
// the state store.
func (r Runner) handleTask(task *datatypes.Task) error {
	task.TimeStarted = uint64(time.Now().Unix())
	task.Status = datatypes.TaskStatus_TASK_IN_PROGRESS

	if err := r.st.SaveProgress(task); err != nil {
		return err
	}

	cmd, err := r.prepareCmd(task)
	if err != nil {
		return err
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Infof("output: %s", string(out))
		return err
	}
	log.V(2).Infof("Output from binary: %s", out)

	if err := saveResults(out, task); err != nil {
		return err
	}

	return nil
}

// prepareCmd prepares a command to run. It gives it input in a correct
// format and returns a exec.Cmd ready to be ran.
func (r Runner) prepareCmd(task *datatypes.Task) (*exec.Cmd, error) {
	// Check is libcerberus library

	if err := sanityCheck(task.GetPayloadPath()); err != nil {
		return nil, errors.Wrap(err, "payload is not a valid libheracles binary")
	}

	var in io.Reader
	var err error

	args := []string{}
	if task.GetKind() == datatypes.TaskKind_MAP {
		args = append(args, "map", fmt.Sprintf("--partition_count=%d", task.GetPartitionCount()))
		in, err = mapReader(task.GetInputChunk())
	} else {
		args = append(args, "reduce")
		in, err = reduceReader(task.GetInputChunk())
	}
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(task.GetPayloadPath(), args...)
	cmd.Stdin = in

	return cmd, nil
}

// fail tasks marks the task as failed, notifies the broker and the state
// store.
func (r Runner) failTask(task *datatypes.Task) error {
	task.TimeDone = uint64(time.Now().Unix())
	task.Status = datatypes.TaskStatus_TASK_FAILED

	if err := r.st.SaveProgress(task); err != nil {
		log.Warningf("unable to save save progress for task %s: %v", task.GetId(), err)
	}
	if err := r.br.Failed(task); err != nil {
		log.Errorf("unable to tell the broker the task %s has failed: %v", task.GetId(), err)
	}
	return nil
}

// succeedTask marks the task as succeeded, notifies the broker and the state
// store, and if it was unable to perform those steps, it fails the task
// instead.
func (r Runner) succeedTask(task *datatypes.Task) error {
	task.TimeDone = uint64(time.Now().Unix())
	task.Status = datatypes.TaskStatus_TASK_DONE

	if err := r.st.SaveProgress(task); err != nil {
		return err
	}
	return errors.Wrap(r.br.Done(task), "can't mark task as done")
}
