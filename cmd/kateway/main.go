package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
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

	if options.ShowVersion {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	if gafka.BuildId == "" {
		fmt.Fprintf(os.Stderr, "empty BuildId, please rebuild with build.sh\n")
	}

	if options.Debug {
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

	if options.KillFile != "" {
		if err := signal.SignalProcessByPidFile(options.KillFile, syscall.SIGUSR2); err != nil {
			panic(err)
		}

		fmt.Println("kateway killed")
		os.Exit(0)
	}

	if options.GolangTrace {
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

	if options.PidFile != "" {
		pid := os.Getpid()
		if err := ioutil.WriteFile(options.PidFile, []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
			panic(err)
		}
	}

	// FIXME logLevel dup with ctx
	setupLogging(options.LogFile, options.LogLevel, options.CrashLogFile)

	// load config
	_, err := os.Stat(options.ConfigFile)
	if err != nil {
		panic(err)
	}
	ctx.LoadConfig(options.ConfigFile)

	ensureValidUlimit()
	debug.SetGCPercent(800) // same env GOGC TODO

	gw := NewGateway(options.Id, options.MetaRefresh)
	if err := gw.Start(); err != nil {
		panic(err)
	}

	gw.ServeForever()

	if options.PidFile != "" {
		syscall.Unlink(options.PidFile)
	}
}
