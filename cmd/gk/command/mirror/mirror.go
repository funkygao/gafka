package mirror

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/golib/ratelimiter"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
)

// target kafka auto.create.topics.enable=true
type Mirror struct {
	Config

	startedAt time.Time
	quit      chan struct{}
	once      sync.Once

	transferN     int64
	transferBytes int64

	bandwidthRateLimiter *ratelimiter.LeakyBucket
}

func New(cf *Config) *Mirror {
	return &Mirror{Config: *cf}
}

func (this *Mirror) Main() (exitCode int) {
	this.quit = make(chan struct{})
	signal.RegisterHandler(func(sig os.Signal) {
		log.Info("received signal: %s", strings.ToUpper(sig.String()))
		log.Info("quiting...")

		this.once.Do(func() {
			close(this.quit)
		})
	}, syscall.SIGINT, syscall.SIGTERM)

	limit := (1 << 20) * this.BandwidthLimit / 8
	if this.BandwidthLimit > 0 {
		this.bandwidthRateLimiter = ratelimiter.NewLeakyBucket(limit*10, time.Second*10)
	}

	log.Info("starting mirror@%s", gafka.BuildId)

	// pprof
	debugAddr := ":10009"
	go http.ListenAndServe(debugAddr, nil)
	log.Info("pprof ready on %s", debugAddr)

	z1 := zk.NewZkZone(zk.DefaultConfig(this.Z1, ctx.ZoneZkAddrs(this.Z1)))
	z2 := zk.NewZkZone(zk.DefaultConfig(this.Z2, ctx.ZoneZkAddrs(this.Z2)))
	c1 := z1.NewCluster(this.C1)
	c2 := z2.NewCluster(this.C2)

	this.runMirror(c1, c2, limit)

	log.Info("bye mirror@%s, %s", gafka.BuildId, time.Since(this.startedAt))
	log.Close()

	return
}

func (this *Mirror) runMirror(c1, c2 *zk.ZkCluster, limit int64) {
	this.startedAt = time.Now()

	log.Info("start [%s/%s] -> [%s/%s] with bandwidth %sbps",
		c1.ZkZone().Name(), c1.Name(),
		c2.ZkZone().Name(), c2.Name(),
		gofmt.Comma(limit*8))

	pub, err := this.makePub(c2)
	if err != nil {
		panic(err)
	}
	log.Trace("pub[%s/%s] made", c2.ZkZone().Name(), c2.Name())

	go func(pub sarama.AsyncProducer, c *zk.ZkCluster) {
		for {
			select {
			case <-this.quit:
				return

			case err := <-pub.Errors():
				// TODO
				log.Error("pub[%s/%s] %v", c.ZkZone().Name(), c.Name(), err)
			}
		}
	}(pub, c2)

	group := this.groupName(c1, c2)
	ever := true
	for ever {
		topics, topicsChanges, err := c1.WatchTopics()
		if err != nil {
			log.Error("[%s/%s]watch topics: %v", c1.ZkZone().Name(), c1.Name(), err)
			time.Sleep(time.Second * 10)
			continue
		}

		// TODO remove '__consumer_offsets' from topics
		sub, err := this.makeSub(c1, group, topics)
		if err != nil {
			// TODO how to handle this err?
			log.Error(err)
			time.Sleep(time.Second * 10)
		}

		log.Info("starting pump [%s/%s] -> [%s/%s] with group %s",
			c1.ZkZone().Name(), c1.Name(),
			c2.ZkZone().Name(), c2.Name(), group)

		pumpStopper := make(chan struct{})
		pumpStopped := make(chan struct{})
		go this.pump(sub, pub, pumpStopper, pumpStopped)

		select {
		case <-topicsChanges:
			// TODO log the diff the topics
			log.Warn("[%s/%s] topics changed, stopping pump...", c1.Name(), c2.Name())
			pumpStopper <- struct{}{} // stop pump
			<-pumpStopped             // await pump cleanup

			// refresh c1 topics
			topics, err = c1.Topics()
			if err != nil {
				// TODO how to handle this err?
				log.Error(err)
				time.Sleep(time.Second * 10)
			}

			log.Info("[%s/%s] topics: %+v", c1.ZkZone().Name(), c1.Name(), topics)

		case <-this.quit:
			log.Info("awaiting pump cleanup...")
			<-pumpStopped

			ever = false

		case <-pumpStopped:
			// pump encounters problems, just retry
			log.Warn("pump stopped for ?")
		}
	}

	log.Info("total transferred: %s %smsgs",
		gofmt.ByteSize(this.transferBytes),
		gofmt.Comma(this.transferN))

	log.Info("closing pub...")
	pub.Close()
}

func (this *Mirror) groupName(c1, c2 *zk.ZkCluster) string {
	return fmt.Sprintf("_mirror_.%s.%s.%s.%s", c1.ZkZone().Name(), c1.Name(), c2.ZkZone().Name(), c2.Name())
}
