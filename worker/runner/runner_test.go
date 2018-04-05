package runner

import (
	"flag"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/pkg/errors"

	"github.com/cpssd/heracles/proto/datatypes"
)

func TestMain(m *testing.M) {
	flag.Bool("-logtostderr", true, "glog flag")
	m.Run()
}

type stubBrokerState struct {
	receiverError error
	tasks         []*datatypes.Task
	taskChan      chan *datatypes.Task
	t             *testing.T
	wg            sync.WaitGroup
}

// Implement broker
func (s *stubBrokerState) Listen() error {
	return s.receiverError
}

func (s *stubBrokerState) Tasks() (<-chan *datatypes.Task, error) {
	return s.taskChan, s.receiverError
}

func (s *stubBrokerState) Done(task *datatypes.Task) error {
	s.t.Logf("done %s", task.GetId())
	s.wg.Done()
	return nil
}

func (s *stubBrokerState) Failed(task *datatypes.Task) error {
	s.t.Logf("failed %s", task.GetId())
	s.wg.Done()
	return nil
}

// Implement state store
func (s *stubBrokerState) SaveProgress(task *datatypes.Task) error {
	return nil
}

// TODO: !!!!! CHANGE THE TEST PATHS TO RELATIVE !!!!
var tasks = []*datatypes.Task{
	{
		Id:             "maptask",
		JobId:          "testjob",
		PayloadPath:    "../../../target/debug/examples/word-counter",
		OutputFiles:    []string{"/tmp/output"},
		PartitionCount: 1,
		Kind:           datatypes.TaskKind_MAP,
	},
	{
		Id:          "reducetask",
		JobId:       "testjob",
		PayloadPath: "../../../target/debug/examples/word-counter",
		OutputFiles: []string{"/tmp/wood"},
		Kind:        datatypes.TaskKind_REDUCE,
	},
}

func TestSanityCheck(t *testing.T) {
	testCases := []struct {
		payloadPath string
		expectGood  bool
	}{
		{
			// TODO: Relative paths
			"../../../target/debug/examples/word-counter",
			true,
		},
		{
			"/bin/ls",
			false,
		},
	}

	for _, test := range testCases {
		if err := sanityCheck(test.payloadPath); err != nil {
			if test.expectGood {
				t.Errorf("error was not expected, but got: %v", err)
			}
			return
		}
		if !test.expectGood {
			t.Error("error was expected, but got none")
		}
	}
}

// TODO: This should actually test the internal errors that can happen if something goes wrong.
func TestRun(t *testing.T) {
	stub := &stubBrokerState{
		tasks: tasks,
		t:     t,
	}

	r := &Runner{
		st: stub,
		br: stub,
	}
	stub.receiverError = errors.New("test")

	if err := r.Run(); err == nil {
		t.Errorf("was expecting %v, got %s", stub.receiverError, err)
		return
	}

	stub.receiverError = nil

	stub.taskChan = make(chan *datatypes.Task, 100)

	blockTillReady := make(chan struct{})
	go func() {
		close(blockTillReady)
		if err := r.Run(); err != nil {
			t.Errorf("was not expecting an error, got %v", err)
			return
		}
	}()
	<-blockTillReady

	for i, task := range stub.tasks {
		f, err := ioutil.TempFile("", "input")
		defer os.Remove(f.Name())
		if err != nil {
			t.Errorf("%d: unable to create the temporary file: %v", i, err)
			return
		}
		if task.GetKind() == datatypes.TaskKind_MAP {
			f.WriteString("how much wood would a wood chuck chuck")
		} else {
			f.WriteString(`[{"key":"wood","value":20},{"key":"wood","value":22}]`)
		}
		fstat, err := f.Stat()
		if err != nil {
			t.Errorf("unable to stat file: %v", err)
			return
		}

		task.InputChunk = &datatypes.InputChunk{
			Path:      f.Name(),
			StartByte: 0,
			EndByte:   uint64(fstat.Size()),
		}

		stub.wg.Add(1)
		stub.taskChan <- task
	}
	close(stub.taskChan)

	stub.wg.Wait()
}
