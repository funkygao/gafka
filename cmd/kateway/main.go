package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/signal"
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

	if options.killFile != "" {
		if err := signal.SignalProcessByPidFile(options.killFile, syscall.SIGUSR2); err != nil {
			panic(err)
		}

		fmt.Println("kateway killed")
		os.Exit(0)
	}

	fmt.Fprintln(os.Stderr, strings.TrimSpace(logo))

	if options.pidFile != "" {
		pid := os.Getpid()
		if err := ioutil.WriteFile(options.pidFile, []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
			panic(err)
		}
	}

	// FIXME logLevel dup with ctx
	setupLogging(options.logFile, options.logLevel, options.crashLogFile)
	ctx.LoadConfig(options.configFile)

	gw := NewGateway(options.id, options.metaRefresh)
	if err := gw.Start(); err != nil {
		panic(err)
	}

	gw.ServeForever()

	if options.pidFile != "" {
		syscall.Unlink(options.pidFile)
	}
}
