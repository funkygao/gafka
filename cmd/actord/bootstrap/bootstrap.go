package bootstrap

import (
	"flag"
	"fmt"
	"io/ioutil"
	golog "log"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/actord/controller"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/meta/zkmeta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/cmd/kateway/store/kafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/gafka/telemetry/influxdb"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

func init() {
	flag.StringVar(&Options.Zone, "z", "", "zone")
	flag.BoolVar(&Options.ShowVersion, "v", false, "show version and exit")
	flag.BoolVar(&Options.ShowVersion, "version", false, "show version and exit")
	flag.StringVar(&Options.LogFile, "log", "stdout", "log file")
	flag.StringVar(&Options.LogLevel, "level", "debug", "log level")
	flag.IntVar(&Options.LogRotateSize, "logsize", 10<<30, "max unrotated log file size")
	flag.StringVar(&Options.InfluxAddr, "influxaddr", "", "influxdb server addr")
	flag.StringVar(&Options.InfluxDbname, "influxdb", "", "influxdb db name")
	flag.Parse()

	if Options.ShowVersion {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	if Options.Zone == "" {
		panic("empty zone not allowed")
	}

	golog.SetOutput(ioutil.Discard)
	if Options.LogFile != "stdout" {
		SetupLogging(Options.LogFile, Options.LogLevel, "panic")
	} else {
		SetupLogging(Options.LogFile, Options.LogLevel, "")
	}

	ctx.LoadFromHome()
}

// Main is the bootstrap main entry point, which will run for ever.
func Main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	var err error
	zkzone := zk.NewZkZone(zk.DefaultConfig(Options.Zone, ctx.ZoneZkAddrs(Options.Zone)))

	// meta pkg is required for store pkg
	metaConf := zkmeta.DefaultConfig()
	metaConf.Refresh = time.Minute * 5
	meta.Default = zkmeta.New(metaConf, zkzone)
	meta.Default.Start()
	log.Trace("meta store[%s] started", meta.Default.Name())

	if Options.InfluxAddr != "" && Options.InfluxDbname != "" {
		rc, err := influxdb.NewConfig(Options.InfluxAddr, Options.InfluxDbname, "", "", time.Minute)
		if err != nil {
			panic(err)
		}
		telemetry.Default = influxdb.New(metrics.DefaultRegistry, rc)
		go func() {
			log.Info("telemetry[%s] started", telemetry.Default.Name())

			if err := telemetry.Default.Start(); err != nil {
				log.Error("telemetry[%s]: %v", telemetry.Default.Name(), err)
			}
		}()
	} else {
		log.Warn("empty influx flag, telemetry disabled")
	}

	var pubStoreWg sync.WaitGroup
	store.DefaultPubStore = kafka.NewPubStore(100, 0, false, &pubStoreWg, false, false)
	if err = store.DefaultPubStore.Start(); err != nil {
		panic(err)
	}
	log.Trace("pub store[%s] started", store.DefaultPubStore.Name())

	c := controller.New(zkzone)
	stopZkWatcher := make(chan struct{})
	go watchZk(c, zkzone, stopZkWatcher)

	signal.RegisterSignalsHandler(func(sig os.Signal) {
		log.Info("actord[%s@%s] received signal: %s", gafka.BuildId, gafka.BuiltAt, strings.ToUpper(sig.String()))

		log.Trace("controller[%s] stopping...", c.Id())
		c.Stop()

	}, syscall.SIGINT, syscall.SIGTERM)

	if err = c.RunForever(); err != nil {
		panic(err)
	}

	// cleanup
	close(stopZkWatcher)

	log.Trace("pub store[%s] stopping", store.DefaultPubStore.Name())
	store.DefaultPubStore.Stop()
	pubStoreWg.Wait()

	meta.Default.Stop()
	log.Trace("meta store[%s] stopped", meta.Default.Name())

	if telemetry.Default != nil {
		telemetry.Default.Stop()
		log.Info("telemetry[%s] stopped", telemetry.Default.Name())
	}

	zkzone.Close()
	log.Trace("zkzone stopped")

	log.Trace("all cleanup done")
}

// keep watch on zk connection jitter
func watchZk(c controller.Controller, zkzone *zk.ZkZone, stop chan struct{}) {
	evtCh, ok := zkzone.SessionEvents()
	if !ok {
		panic("someone else is consuming my zk events?")
	}

	// during connecting phase, the following events are fired:
	// StateConnecting -> StateConnected -> StateHasSession
	firstHandShaked := false
	for {
		select {
		case <-stop:
			return

		case evt := <-evtCh:
			if !firstHandShaked {
				if evt.State == zklib.StateHasSession {
					firstHandShaked = true
				}

				continue
			}

			log.Warn("zk jitter: %+v", evt)

			if evt.State == zklib.StateHasSession {
				log.Warn("zk reconnected after session lost, watcher/ephemeral lost")

				zkzone.CallSOS(fmt.Sprintf("actord[%s]", c.Id()), "zk session expired")
			}
		}
	}

}
