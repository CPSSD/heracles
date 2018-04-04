package main

import (
	"fmt"
	"os"

	"github.com/cpssd/heracles/worker-fallback/worker"
)

func main() {
	if err := worker.Main(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
