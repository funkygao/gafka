package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/debug"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/profiler"
)

func init() {
	parseFlags()

	if options.showVersion {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	if options.debug {
		log.SetFlags(log.LstdFlags | log.Llongfile) // TODO zk sdk uses this
		log.SetPrefix(color.Magenta("[log]"))

		sarama.Logger = log.New(os.Stdout, color.Green("[Sarama]"),
			log.LstdFlags|log.Lshortfile)
	} else {
		log.SetOutput(ioutil.Discard)
	}

}

func main() {
	validateFlags()
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
    _/    _/              _/                                                        
   _/  _/      _/_/_/  _/_/_/_/    _/_/    _/      _/      _/    _/_/_/  _/    _/   
  _/_/      _/    _/    _/      _/_/_/_/  _/      _/      _/  _/    _/  _/    _/    
 _/  _/    _/    _/    _/      _/          _/  _/  _/  _/    _/    _/  _/    _/     
_/    _/    _/_/_/      _/_/    _/_/_/      _/      _/        _/_/_/    _/_/_/      
                                                                           _/       
                                                                      _/_/          
	`))

	setupLogging(options.logFile, options.logLevel, options.crashLogFile)
	ctx.LoadConfig(options.configFile)

	gw := NewGateway(options.metaRefresh)
	if err := gw.Start(); err != nil {
		panic(err)
	}

	gw.ServeForever()

}
