package splitting

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cpssd/heracles/proto/datatypes"
)

// Reduce splitting
func Reduce(job *datatypes.Job, interm map[int][]string) []*datatypes.Task {
	tasks := []*datatypes.Task{}
	for i, output := range job.GetOutputFiles() {
		id := uuid.New().String()

		task := &datatypes.Task{
			Id:          id,
			JobId:       job.GetId(),
			Status:      datatypes.TaskStatus_TASK_PENDING,
			Kind:        datatypes.TaskKind_REDUCE,
			TimeCreated: uint64(time.Now().Unix()),
			OutputFiles: []string{output},
			PayloadPath: job.GetPayloadPath(),
			InputChunk: &datatypes.InputChunk{
				Path: strings.Join(interm[i], ","),
			},
		}
		tasks = append(tasks, task)
	}
	return tasks
}
