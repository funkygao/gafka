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
	"github.com/funkygao/gafka/cmd/kateway/gateway"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/signal"
	glog "github.com/funkygao/log4go"
)

func init() {
	gateway.ParseFlags()
	if !gateway.Options.RunSwaggerServer {
		gateway.ValidateFlags()
	}

	gateway.EnsureServerUlimit()
	debug.SetGCPercent(800) // same env GOGC. in golang1.7, we needn't concern about this
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	if gateway.Options.RunSwaggerServer {
		runSwaggerServer()
		return
	}

	if gateway.Options.Debug {
		log.SetFlags(log.LstdFlags | log.Llongfile) // TODO zk sdk uses this
		log.SetPrefix(color.Magenta("[log]"))
	} else {
		log.SetOutput(ioutil.Discard)
	}

	if gateway.Options.KillFile != "" {
		if err := signal.SignalProcessByPidFile(gateway.Options.KillFile, syscall.SIGUSR2); err != nil {
			panic(err)
		}

		fmt.Println("kateway killed")
		os.Exit(0)
	}

	if gateway.Options.GolangTrace {
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

	if gateway.Options.PidFile != "" {
		pid := os.Getpid()
		if err := ioutil.WriteFile(gateway.Options.PidFile, []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
			panic(err)
		}
	}

	gateway.SetupLogging(gateway.Options.LogFile, gateway.Options.LogLevel, gateway.Options.CrashLogFile)

	// load config
	if gateway.Options.ConfigFile == "" {
		ctx.LoadFromHome()
	} else {
		_, err := os.Stat(gateway.Options.ConfigFile)
		if err != nil {
			panic(err)
		}
		ctx.LoadConfig(gateway.Options.ConfigFile)
	}

	gw := gateway.New(gateway.Options.Id)
	if err := gw.Start(); err != nil {
		glog.Error(err.Error())
		glog.Info("kateway[%s@%s] bye!", gafka.BuildId, gafka.BuiltAt)
		glog.Close()
		os.Exit(1)
	}

	t0 := time.Now()
	gw.ServeForever()
	glog.Info("kateway[%s@%s] %s, bye!", gafka.BuildId, gafka.BuiltAt, time.Since(t0))
	glog.Close()

	if gateway.Options.PidFile != "" {
		syscall.Unlink(gateway.Options.PidFile)
	}
}
