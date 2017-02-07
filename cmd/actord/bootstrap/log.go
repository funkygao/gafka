package bootstrap

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/funkygao/gafka"
	log "github.com/funkygao/log4go"
)

var (
	logLevel log.Level
)

func SetupLogging(logFile, level, crashLogFile string) {
	logLevel = log.ToLogLevel(level, log.TRACE)
	log.SetLevel(logLevel)
	log.LogBufferLength = 10 << 10 // default 32, chan cap

	if logFile != "stdout" {
		log.DeleteFilter("stdout")

		rotateEnabled, discardWhenDiskFull := true, false
		filer := log.NewFileLogWriter(logFile, rotateEnabled, discardWhenDiskFull, 0644)
		filer.SetFormat("[%d %T] [%L] (%S) %M")
		if Options.LogRotateSize > 0 {
			filer.SetRotateSize(Options.LogRotateSize)
		}
		filer.SetRotateKeepDuration(time.Hour * 24 * 30)
		filer.SetRotateLines(0)
		filer.SetRotateDaily(true)
		log.AddFilter("file", logLevel, filer)
	}

	if crashLogFile != "" {
		f, err := os.OpenFile(crashLogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}

		syscall.Dup2(int(f.Fd()), int(os.Stdout.Fd()))
		syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd()))
		fmt.Fprintf(os.Stderr, "\n%s %s (build: %s)\n===================\n",
			time.Now().String(),
			gafka.Version, gafka.BuildId)
	}

}
