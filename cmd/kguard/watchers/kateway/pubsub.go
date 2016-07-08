package kateway

import (
	"bytes"
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
			pubsubHealth.Update(int64(1))
		}
	}
}

func (this *WatchPubsub) runCheckup() {
	kws, err := this.Zkzone.KatewayInfos()
	if err != nil {
		// TODO
	}

	var (
		myApp         = os.Getenv("MYAPP")
		hisApp        = os.Getenv("HISAPP")
		secret        = os.Getenv("APPKEY")
		ver    string = "v1"
		topic  string = "smoketestonly"
		group         = "__smoketestonly__"
	)

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
			// TODO
		}

		// confirm that sub can get the pub'ed message
		cli.Sub(api.SubOption{
			AppId: hisApp,
			Topic: topic,
			Ver:   ver,
			Group: group,
		}, func(statusCode int, subMsg []byte) error {
			if statusCode != http.StatusOK {
				// TODO
			}
			if !bytes.Equal(pubMsg, subMsg) {
				// TODO
			}

			return api.ErrSubStop
		})

	}
}
