package bootstrap

import (
	"flag"
	"fmt"
	"io/ioutil"
	golog "log"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/actord/controller"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/meta/zkmeta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/cmd/kateway/store/kafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

func init() {
	flag.StringVar(&Options.Zone, "z", "", "zone")
	flag.BoolVar(&Options.ShowVersion, "v", false, "show version and exit")
	flag.BoolVar(&Options.ShowVersion, "version", false, "show version and exit")
	flag.Parse()

	if Options.ShowVersion {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	if Options.Zone == "" {
		panic("empty zone not allowed")
	}

	golog.SetOutput(ioutil.Discard)

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

	zkzone := zk.NewZkZone(zk.DefaultConfig(Options.Zone, ctx.ZoneZkAddrs(Options.Zone)))

	// TODO signals

	metaConf := zkmeta.DefaultConfig()
	metaConf.Refresh = time.Minute * 5
	meta.Default = zkmeta.New(metaConf, zkzone)
	meta.Default.Start()

	var wg sync.WaitGroup
	store.DefaultPubStore = kafka.NewPubStore(100, 0, false, &wg, false, false)
	if err := store.DefaultPubStore.Start(); err != nil {
		panic(err)
	}

	c := controller.New(zkzone)
	go watchZk(c, zkzone)

	if err := c.ServeForever(); err != nil {
		panic(err)
	}

	wg.Wait()

}

// keep watch on zk connection jitter
func watchZk(c controller.Controller, zkzone *zk.ZkZone) {
	evtCh, ok := zkzone.SessionEvents()
	if !ok {
		panic("someone else is consuming my zk events?")
	}

	// during connecting phase, the following events are fired:
	// StateConnecting -> StateConnected -> StateHasSession
	firstHandShaked := false
	for evt := range evtCh {
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
