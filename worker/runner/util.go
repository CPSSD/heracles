// util functions used when running the payload.

package runner

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"

	log "github.com/golang/glog"
	"github.com/pkg/errors"

	"github.com/cpssd/heracles/proto/datatypes"
)

// sanityCheck checks is a payload a valid libcerberus/libheracles
// binary.
func sanityCheck(payloadPath string) error {
	out, err := exec.Command(payloadPath, "sanity-check").CombinedOutput()
	if err != nil || string(out) != "sanity located" {
		return errors.Wrap(err, "sanity check failed")
	}
	return nil
}

// mapReader takes in the input chunk and packages it up to map compabible
// input format.
func mapReader(in *datatypes.InputChunk) (io.Reader, error) {
	start := in.GetStartByte()
	end := in.GetEndByte()

	f, err := os.Open(in.GetPath())
	if err != nil {
		return nil, errors.Wrap(err, "unable to open input file")
	}

	// This is a human convenience change. If the end is not specified, we
	// just go until the end of file.
	if end == 0 {
		if fstat, err := f.Stat(); err != nil {
			log.Warning("unable to get end of file: %v", err)
		} else {
			end = uint64(fstat.Size())
		}
		log.V(1).Infof("end not specified: changing from 0 to %d", end)
	}

	buf := make([]byte, end-start)
	if _, err := f.ReadAt(buf, int64(start)); err != nil && err != io.EOF {
		return nil, errors.Wrap(err, "unable to read input data")
	}

	out, err := json.Marshal(&kv{Key: in.GetPath(), Value: string(buf)})
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse map input")
	}
	log.V(2).Infof("map input: %s", out)

	return bytes.NewBuffer(out), nil
}

type reducerKVs struct {
	Key    string        `json:"key"`
	Values []interface{} `json:"values"`
}

type reducerInput []reducerKVs

func reduceReader(in *datatypes.InputChunk) (io.Reader, error) {
	// we can assume that the whole input is for the reducer, so we just ignore
	// the start and end bytes
	f, err := os.Open(in.GetPath())
	if err != nil {
		return nil, errors.Wrap(err, "unable to open input file")
	}

	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read input data")
	}

	kvs := []kv{}
	if err := json.Unmarshal(buf, &kvs); err != nil {
		return nil, errors.Wrap(err, "unable to parse JSON")
	}

	tmp := make(map[string][]interface{})
	for _, kv := range kvs {
		tmp[kv.Key] = append(tmp[kv.Key], kv.Value)
	}

	data := reducerInput{}
	for key, values := range tmp {
		data = append(data, reducerKVs{
			Key:    key,
			Values: values,
		})
	}

	out, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse reduce input")
	}
	log.V(2).Infof("reduce input: %s", out)

	return bytes.NewReader(out), nil
}

type kv struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type mapOutputFormat struct {
	Partitions map[string][]kv `json:"partitions"`
}

// saveResults takes in the bytes of the output, interprets them, and saves
// them into required output files.
func saveResults(in []byte, task *datatypes.Task) error {
	switch task.GetKind() {
	case datatypes.TaskKind_MAP:
		return saveMapResults(in, task.GetOutputFiles())
	case datatypes.TaskKind_REDUCE:
		return saveReduceResults(in, task.GetOutputFiles())
	}

	return errors.New("task type not valid")
}

func saveMapResults(in []byte, output []string) error {
	data := &mapOutputFormat{}
	if err := json.Unmarshal(in, &data); err != nil {
		return errors.Wrap(err, "unable to parse JSON")
	}

	for partitionName, kvPairs := range data.Partitions {
		partition, err := strconv.Atoi(partitionName)
		if err != nil {
			return errors.Wrap(err, "unable to convert partition name")
		}

		filePath := output[partition]
		log.Info(filePath)
		pairsBytes, err := json.Marshal(kvPairs)
		if err != nil {
			return errors.Wrap(err, "unable to reserialize pairs")
		}
		if err := ioutil.WriteFile(filePath, pairsBytes, 0644); err != nil {
			return errors.Wrap(err, "unable to write the file")
		}
	}

	return nil
}

type reducerOutputFormat []struct {
	Values []interface{} `json:"values"`
}

func saveReduceResults(in []byte, outputFiles []string) error {
	// TODO: probably add some logic to divide the output keys to their
	// 		 respectable output files.

	if len(outputFiles) == 0 {
		return errors.New("output files cannot be empty")
	}

	return ioutil.WriteFile(outputFiles[0], in, 0644)
}
