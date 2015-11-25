package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
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

	fmt.Fprintln(os.Stderr, strings.TrimSpace(`
                         _/_/  _/                  
     _/_/_/    _/_/_/    _/      _/  _/      _/_/_/   
  _/    _/  _/    _/  _/_/_/_/  _/_/      _/    _/    
 _/    _/  _/    _/    _/      _/  _/    _/    _/     
  _/_/_/    _/_/_/    _/      _/    _/    _/_/_/      
     _/                                               
_/_/                                                  
	`))

	setupLogging(options.logFile, options.logLevel, options.crashLogFile)
	ctx.LoadConfig(options.configFile)

	gw := NewGateway()

	signal.RegisterSignalHandler(syscall.SIGINT, func(sig os.Signal) {
		gw.Stop()

		log.Info("Terminated")
		os.Exit(0)
	})

	log.Info("pubd started")

	go runSysStats(time.Now(), options.tick)

	gw.BuildRouting()
	gw.Start()
	gw.ServeForever()

}
