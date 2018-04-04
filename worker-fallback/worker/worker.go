package worker

import (
	"github.com/golang/glog"
	log "github.com/golang/glog"
	"github.com/pkg/errors"

	"github.com/cpssd/heracles/worker-fallback/worker/broker"
	"github.com/cpssd/heracles/worker-fallback/worker/runner"
	"github.com/cpssd/heracles/worker-fallback/worker/settings"
	"github.com/cpssd/heracles/worker-fallback/worker/state"
)

// Main entrypoint for the worker fallback.
func Main() error {
	if err := settings.Init(); err != nil {
		return errors.Wrap(err, "unable to initialize settings")
	}

	glog.Info("starting heracles worker fallback")

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
