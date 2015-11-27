package main

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/funkygao/gafka"
	log "github.com/funkygao/log4go"
)

func setupLogging(logFile, logLevel, crashLogFile string) {
	level := log.DEBUG
	switch logLevel {
	case "info":
		level = log.INFO

	case "warn":
		level = log.WARNING

	case "error":
		level = log.ERROR

	case "debug":
		level = log.DEBUG

	case "trace":
		level = log.TRACE

	case "alarm":
		level = log.ALARM
	}

	for _, filter := range log.Global {
		filter.Level = level
	}

	log.LogBufferLength = 1 << 10 // default 32, chan cap

	if logFile == "stdout" {
		log.AddFilter("stdout", level, log.NewConsoleLogWriter())
	} else {
		log.DeleteFilter("stdout")

		filer := log.NewFileLogWriter(logFile, false)
		filer.SetFormat("[%d %T] [%L] (%S) %M")
		filer.SetRotate(true)
		filer.SetRotateSize(0)
		filer.SetRotateLines(0)
		filer.SetRotateDaily(true)
		log.AddFilter("file", level, filer)
	}

	if crashLogFile != "" {
		f, err := os.OpenFile(crashLogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}

		syscall.Dup2(int(f.Fd()), 2)
		fmt.Fprintf(os.Stderr, "\n%s %s (build: %s)\n===================\n",
			time.Now().String(),
			gafka.Version, gafka.BuildId)
	}

}
