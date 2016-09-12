package kateway

import (
	"fmt"
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

var errKatewayAllGone = fmt.Errorf("all kateway gone")

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

	startedAt time.Time
	seq       int

	pubLatency, subLatency metrics.Histogram
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

	this.startedAt = time.Now()
	pubsubHealth := metrics.NewRegisteredGauge("kateway.pubsub.fail", nil)
	this.pubLatency = metrics.NewRegisteredHistogram("kateway.pubsub.latency.pub", nil, metrics.NewExpDecaySample(1028, 0.015))
	this.subLatency = metrics.NewRegisteredHistogram("kateway.pubsub.latency.sub", nil, metrics.NewExpDecaySample(1028, 0.015))

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

	if len(kws) == 0 {
		log.Error("%s", errKatewayAllGone)
		return errKatewayAllGone
	}

	var (
		myApp  = os.Getenv("MYAPP")
		hisApp = os.Getenv("HISAPP")
		secret = os.Getenv("APPKEY")
		ver    = "v1"
		topic  = "smoketestonly"
		group  = "__smoketestonly__"

		pubEndpoint = os.Getenv("PUB")
		subEndpoint = os.Getenv("SUB")
	)

	if myApp == "" || hisApp == "" || secret == "" {
		log.Error("empty pubsub params provided")
		return nil
	}

	if pubEndpoint != "" && subEndpoint != "" {
		// add the load balancer endpoint
		kws = append(kws, &zk.KatewayMeta{
			Id:      "0",
			PubAddr: pubEndpoint,
			SubAddr: subEndpoint,
		})
	}

	for _, kw := range kws {
		// pub a message
		cf := api.DefaultConfig(myApp, secret)
		cf.Pub.Endpoint = kw.PubAddr
		cf.Sub.Endpoint = kw.SubAddr
		cli := api.NewClient(cf)
		this.seq++
		pubMsg := fmt.Sprintf("kguard smoke test msg: [%s/%d]", this.startedAt, this.seq)

		t0 := time.Now()
		err = cli.Pub("", []byte(pubMsg), api.PubOption{
			Topic: topic,
			Ver:   ver,
		})
		if err != nil {
			log.Error("pub[%s]: %v", kw.Id, err)
			return err
		}
		this.pubLatency.Update(time.Since(t0).Nanoseconds() / 1e6) // in ms

		t0 = time.Now()

		// confirm that sub can get the pub'ed message
		err = cli.Sub(api.SubOption{
			AppId:     hisApp,
			Topic:     topic,
			Ver:       ver,
			Group:     group,
			AutoClose: true,
		}, func(statusCode int, subMsg []byte) error {
			if statusCode != http.StatusOK {
				return fmt.Errorf("unexpected http status: %s", http.StatusText(statusCode))
			}
			if len(subMsg) < 10 {
				log.Warn("unexpected sub msg: %s", string(subMsg))
			}

			return api.ErrSubStop
		})
		if err != nil {
			log.Error("sub[%s]: %v", kw.Id, err)
			return err
		}

		this.subLatency.Update(time.Since(t0).Nanoseconds() / 1e6) // in ms

		// wait for server cleanup the sub conn
		time.Sleep(time.Second)
	}

	return nil
}
