package scheduler

import (
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/cpssd/heracles/manager-fallback/broker"
	"github.com/cpssd/heracles/manager-fallback/splitting"
	"github.com/cpssd/heracles/manager-fallback/state"
	"github.com/cpssd/heracles/proto/datatypes"
)

// Scheduler type
type Scheduler struct {
	st   state.State
	br   broker.Broker
	jobs chan *datatypes.Job
}

// New Scheduler
func New(br broker.Broker, st state.State) *Scheduler {
	return &Scheduler{
		br:   br,
		st:   st,
		jobs: make(chan *datatypes.Job),
	}
}

// Schedule a job
func (s Scheduler) Schedule(job *datatypes.Job) (string, error) {
	id := uuid.New().String()
	job.Id = id
	if err := s.st.SaveJob(job); err != nil {
		return "", errors.Wrap(err, "can't schedule")
	}

	s.jobs <- job

	return id, nil
}

// Cancel a job
func (s Scheduler) Cancel(jobID string) error {
	return nil
}

// ProcessJobs listens
func (s Scheduler) ProcessJobs() {
	log.Info("begining to listen for any jobs")
	var wg sync.WaitGroup
	for job := range s.jobs {
		wg.Add(1)
		go func(job *datatypes.Job) {
			defer wg.Done()
			if err := s.processJob(job); err != nil {
				log.Errorf("error processing job: %v", err)
			}
		}(job)
	}
	wg.Wait()

}

func (s *Scheduler) processJob(job *datatypes.Job) error {
	var wg sync.WaitGroup

	intermediateFiles := splitting.IntermediateFiles(job)

	// create all tasks
	mapTasks, err := splitting.Map(job, intermediateFiles)
	if err != nil {
		job.FailureDetails = err.Error()
		return err
	}
	reduceTasks := splitting.Reduce(job, intermediateFiles)

	if err := s.st.CreateTasks(append(mapTasks, reduceTasks...)); err != nil {
		job.FailureDetails = err.Error()
		return err
	}

	// Start tunning the reduce tasks
	wg.Add(len(mapTasks))
	log.V(1).Info("starting map tasks")
	for _, task := range mapTasks {
		go func(task *datatypes.Task) {
			defer wg.Done()
			if err := s.processTask(task); err != nil {
				job.FailureDetails = err.Error()
				job.Status = datatypes.JobStatus_JOB_FAILED
				log.Errorf("%v", err)
				return
			}
		}(task)
	}
	wg.Wait()
	s.st.WaitUntilTasksComplete(job.GetId(), datatypes.TaskKind_MAP)

	wg.Add(len(reduceTasks))
	log.V(1).Info("starting reduce tasks")
	for _, task := range reduceTasks {
		go func(task *datatypes.Task) {
			defer wg.Done()
			if err := s.processTask(task); err != nil {
				job.FailureDetails = err.Error()
				job.Status = datatypes.JobStatus_JOB_FAILED
				log.Errorf("%v", err)
				return
			}
		}(task)
	}
	wg.Wait()
	s.st.WaitUntilTasksComplete(job.GetId(), datatypes.TaskKind_REDUCE)

	job.Status = datatypes.JobStatus_JOB_DONE
	job.TimeDone = uint64(time.Now().Unix())
	return s.st.SaveJob(job)
}

func (s *Scheduler) processTask(task *datatypes.Task) error {
	log.Infof("processing task %s: Sending to broker", task.GetId())
	if err := s.br.Send(task); err != nil {
		return errors.Wrap(err, "can't send task to broker")
	}

	return nil
}
