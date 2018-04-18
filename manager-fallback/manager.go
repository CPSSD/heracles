package main

import (
	"fmt"
	"os"

	log "github.com/golang/glog"
	"github.com/pkg/errors"

	"github.com/cpssd/heracles/manager-fallback/broker"
	"github.com/cpssd/heracles/manager-fallback/scheduler"
	"github.com/cpssd/heracles/manager-fallback/server"
	"github.com/cpssd/heracles/manager-fallback/settings"
	"github.com/cpssd/heracles/manager-fallback/state"
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

	log.Info("starting heracles manager-fallback")

	st, err := state.New()
	if err != nil {
		return errors.Wrap(err, "unable to connect to state")
	}
	br, err := broker.New()
	if err != nil {
		return errors.Wrap(err, "unable to create a broker connection")
	}

	sch := scheduler.New(br, st)

	waitUntilProcessingReady := make(chan struct{})
	go func() {
		close(waitUntilProcessingReady)
		sch.ProcessJobs()
	}()
	<-waitUntilProcessingReady

	if err := server.New(sch).Run(); err != nil {
		return errors.Wrap(err, "error running server")
	}

	return nil
}
