package main

import (
	"os"

	log "github.com/funkygao/log4go"
)

func shutdown() {
	log.Info("Terminated")
	os.Exit(0)
}
