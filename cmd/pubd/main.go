package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/profiler"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
)

func init() {
	parseFlags()

	if options.showVersion {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

}

func main() {
	defer func() {
		shutdown()

		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	if options.cpuprof || options.memprof {
		cf := &profiler.Config{
			Quiet:        true,
			ProfilePath:  "prof",
			CPUProfile:   options.cpuprof,
			MemProfile:   options.memprof,
			BlockProfile: options.blockprof,
		}

		defer profiler.Start(cf).Stop()
	}

	fmt.Fprintln(os.Stderr, `
     ____      __      ____ 
    ( ___)    /__\    ( ___)
     )__)    /(__)\    )__) 
    (__)    (__)(__)  (____)`)

	setupLogging(options.logFile, options.logLevel, options.crashLogFile)
	ctx.LoadConfig(options.configFile)

	signal.RegisterSignalHandler(syscall.SIGINT, func(sig os.Signal) {
		shutdown()
	})

	log.Info("pubd started")

	go runSysStats(time.Now(), options.tick)

	gw := &PubGateway{}
	gw.ServeForever()

	log.Info("ha")

	time.Sleep(time.Hour)
}
