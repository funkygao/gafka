package kateway

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kateway.pubsub", func() monitor.Watcher {
		return &WatchPubsub{
			Tick: time.Minute,
		}
	})
}

// WatchPubsub monitors aliveness of kateway cluster.
type WatchPubsub struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchPubsub) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchPubsub) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	pubsubHealth := metrics.NewRegisteredGauge("kateway.pubsub", nil)

	for {
		select {
		case <-this.Stop:
			log.Info("kateway.pubsub stopped")
			return

		case <-ticker.C:
			if err := this.runCheckup(); err != nil {
				pubsubHealth.Update(1)
			} else {
				pubsubHealth.Update(0)
			}
		}
	}
}

func (this *WatchPubsub) runCheckup() error {
	kws, err := this.Zkzone.KatewayInfos()
	if err != nil {
		log.Error("pubsub: %v", err)
		return err
	}

	var (
		myApp  = os.Getenv("MYAPP")
		hisApp = os.Getenv("HISAPP")
		secret = os.Getenv("APPKEY")
		ver    = "v1"
		topic  = "smoketestonly"
		group  = "__smoketestonly__"
	)

	if myApp == "" || hisApp == "" || secret == "" {
		log.Error("empty pubsub params provided")
		return nil
	}

	for _, kw := range kws {
		// pub a message
		cf := api.DefaultConfig(myApp, secret)
		cf.Pub.Endpoint = kw.PubAddr
		cf.Sub.Endpoint = kw.SubAddr
		cli := api.NewClient(cf)
		msgId := rand.Int()
		pubMsg := fmt.Sprintf("smoke test[%d] from kguard", msgId)

		err = cli.Pub("", []byte(pubMsg), api.PubOption{
			Topic: topic,
			Ver:   ver,
		})
		if err != nil {
			log.Error("pub[%s]: %v", kw.Id, err)
			return err
		}

		// confirm that sub can get the pub'ed message
		err = cli.Sub(api.SubOption{
			AppId:     hisApp,
			Topic:     topic,
			Ver:       ver,
			Group:     group,
			AutoClose: true,
		}, func(statusCode int, subMsg []byte) error {
			if statusCode != http.StatusOK {
				log.Error("sub status: %s", http.StatusText(statusCode))
			} else {
				log.Info("sub[%s]: %s", kw.Id, string(subMsg))
			}

			return api.ErrSubStop
		})

		if err != nil {
			log.Error("sub[%s]: %v", kw.Id, err)
			return err
		}

	}

	return nil
}
