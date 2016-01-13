package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"strings"
	"syscall"
	"time"

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

	if gafka.BuildId == "" {
		fmt.Fprintf(os.Stderr, "empty BuildId, please rebuild with build.sh\n")
		os.Exit(1)
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

	if options.golangTrace {
		// go tool trace kateway xxx.pprof
		f, err := os.Create(time.Now().Format("2006-01-02T150405.pprof"))
		if err != nil {
			panic(err)
		}
		defer f.Close()

		if err = trace.Start(f); err != nil {
			panic(err)
		}
		defer trace.Stop()
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

	ensureValidUlimit()
	debug.SetGCPercent(800) // same env GOGC TODO

	gw := NewGateway(options.id, options.metaRefresh)
	if err := gw.Start(); err != nil {
		panic(err)
	}

	gw.ServeForever()

	if options.pidFile != "" {
		syscall.Unlink(options.pidFile)
	}
}
