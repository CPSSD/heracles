package splitting

import (
	"io/ioutil"
	"os"
	"path"
	"time"

	log "github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/cpssd/heracles/manager-fallback/settings"
	"github.com/cpssd/heracles/proto/datatypes"
)

// Map Splitting
func Map(job *datatypes.Job, interm map[int][]string) ([]*datatypes.Task, error) {
	switch job.GetInputKind() {
	case datatypes.InputDataKind_UNDEFINED:
		return nil, errors.New("type cannot be undefined")
	case datatypes.InputDataKind_DATA_TEXT_NEWLINES:
		return textMap(job, interm)
	}
	return nil, nil
}

func textMap(job *datatypes.Job, interm map[int][]string) ([]*datatypes.Task, error) {
	entries, err := ioutil.ReadDir(job.GetInputDirectory())
	if err != nil {
		return nil, errors.Wrap(err, "can't read dir")
	}

	var chunks []*datatypes.InputChunk
	for _, entry := range entries {
		chunks = append(chunks,
			splitTextFile(path.Join(job.GetInputDirectory(), entry.Name()))...,
		)
	}

	tasks := []*datatypes.Task{}
	for i, chunk := range chunks {
		id := uuid.New().String()

		outputFiles := []string{}
		for p := range interm {
			outputFiles = append(outputFiles, interm[p][i])
		}

		task := &datatypes.Task{
			Id:             id,
			JobId:          job.GetId(),
			Status:         datatypes.TaskStatus_TASK_PENDING,
			Kind:           datatypes.TaskKind_MAP,
			TimeCreated:    uint64(time.Now().Unix()),
			InputChunk:     chunk,
			PayloadPath:    job.GetPayloadPath(),
			PartitionCount: uint64(len(job.GetOutputFiles())),
			OutputFiles:    outputFiles,
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}

// The functions returns a bunch (or one) chunk from input. It does so by doing
// the following:
//		if the file is below the threshold, it returns the whole file
//		if the file is above the threshold, it does the following:
//			goes to threshold
//			traces back until a new line character is found, creates chunk from
//				that
//			goes from that point where the new line char has been found and
//			tries from the beginning
func splitTextFile(f string) []*datatypes.InputChunk {
	fi, err := os.Stat(f)
	if err != nil {
		// Nothing to do about it . just log
		log.Error(err)
	}

	log.V(2).Infof("Splitting file %s", fi.Name())

	max := int64(settings.Int("scheduler.input_chunk_size"))

	if fi.Size() < max {
		return []*datatypes.InputChunk{
			{
				Path:      f,
				StartByte: 0,
				EndByte:   uint64(fi.Size()),
			},
		}
	}

	chunks := []*datatypes.InputChunk{}

	// TODO: Chunks bigger than 64MB

	return chunks
}
