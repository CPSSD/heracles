package main

import (
	"fmt"
	"os"

	"github.com/cpssd/heracles/cmd/hrctl/app"
)

func main() {
	if err := app.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
