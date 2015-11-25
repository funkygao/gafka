package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/profiler"
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

	gw := NewGateway(options.mode, options.metaRefresh)
	if err := gw.Start(); err != nil {
		panic(err)
	}

	gw.ServeForever()

}
