package main

import (
	log "github.com/funkygao/log4go"
)

func init() {
	log.AddFilter("stdout", log.INFO, log.NewConsoleLogWriter())
}

func main() {
	var m Monitor
	m.Init()
	m.ServeForever()
}
