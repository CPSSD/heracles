package splitting

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"

	log "github.com/golang/glog"
	"github.com/google/uuid"

	"github.com/cpssd/heracles/manager-fallback/settings"
	"github.com/cpssd/heracles/proto/datatypes"
)

func fileRange(id string, num int) []string {
	dir := settings.String("scheduler.intermediate_data_location")

	// create the dir
	os.MkdirAll(path.Join(dir, id), os.ModePerm)

	ret := []string{}
	for i := 0; i < num; i++ {
		ret = append(ret, path.Join(dir, id, strconv.Itoa(i)))
	}
	return ret
}

// IntermediateFiles returns the names of intermediate files.
func IntermediateFiles(job *datatypes.Job) map[int][]string {
	out := len(job.GetOutputFiles())

	// create the intermediate directory
	jobDir := path.Join(settings.String("scheduler.intermediate_data_location"), job.GetId())
	if err := os.MkdirAll(jobDir, os.ModePerm); err != nil {
		log.Errorf("can't create intermediata job directory: %v", err)
		return nil
	}

	inFiles, err := ioutil.ReadDir(job.GetInputDirectory())
	if err != nil {
		log.Error(err)
		return nil
	}

	ret := make(map[int][]string)

	for i := 0; i < out; i++ {
		if err := os.MkdirAll(path.Join(jobDir, strconv.Itoa(i)), os.ModePerm); err != nil {
			log.Errorf("can't create intermediate data dir partition %d", i)
			return nil
		}

		for range inFiles {
			ret[i] = append(ret[i], path.Join(jobDir, strconv.Itoa(i), uuid.New().String()))
		}
	}

	return ret
}
