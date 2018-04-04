package app

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	dpb "github.com/cpssd/heracles/proto/datatypes"
)

// Errors
var (
	errEmptyJobPath   = "job path is empty"
	errUnmarshaling   = "unable to unmarshal job from file"
	errEmptyInputDir  = "input directory cannot be empty"
	errEmptyPayload   = "payload path cannot be empty"
	errInvalidSepator = "cannot use undefined input separator"
)

// Run the application
func Run() error {
	return parse().Run(os.Args)
}

func loadJob(jobPath string) (*dpb.Job, error) {
	if jobPath == "" {
		return nil, errors.New(errEmptyJobPath)
	}

	data, err := ioutil.ReadFile(path.Clean(jobPath))
	if err != nil {
		return nil, err
	}

	job := &dpb.Job{}
	if err := proto.UnmarshalText(string(data), job); err != nil {
		return nil, errors.Wrap(err, errUnmarshaling)
	}

	// Check required arguments
	if job.GetInputDirectory() == "" {
		return nil, errors.New(errEmptyInputDir)
	}
	if job.GetPayloadPath() == "" {
		return nil, errors.New(errEmptyPayload)
	}
	if job.GetInputKind() == dpb.InputDataKind_UNDEFINED {
		return nil, errors.New(errInvalidSepator)
	}
	return job, nil
}
