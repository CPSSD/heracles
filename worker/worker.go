package main

import (
	"fmt"
	"os"

	log "github.com/golang/glog"
	"github.com/pkg/errors"

	"github.com/cpssd/heracles/worker/broker"
	"github.com/cpssd/heracles/worker/runner"
	"github.com/cpssd/heracles/worker/settings"
	"github.com/cpssd/heracles/worker/state"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
}

func run() error {
	if err := settings.Init(); err != nil {
		return errors.Wrap(err, "unable to initialize settings")
	}

	log.Info("starting heracles worker")

	st, err := state.New()
	if err != nil {
		return errors.Wrap(err, "unable to connect to state")
	}
	br, err := broker.New()
	if err != nil {
		return errors.Wrap(err, "unable to create a broker connection")
	}

	// TODO: We  probably might want to recover instead.
	waitUntilListening := make(chan bool)
	defer close(waitUntilListening)
	go func() {
		waitUntilListening <- true
		if err := br.Listen(); err != nil {
			log.Fatalf("unable to listen to broker: %v", err)
		}
		return
	}()
	<-waitUntilListening

	log.Info("starting runner")

	// Run starts listening and holds.
	if err := runner.New(st, br).Run(); err != nil {
		return errors.Wrap(err, "error running a command")
	}

	return nil
}
