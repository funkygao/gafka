package kafka

import (
	l "log"
	"os"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/kafka-cg/consumergroup"
	log "github.com/funkygao/log4go"
)

type subStore struct {
	shutdownCh   chan struct{}
	closedConnCh <-chan string // remote addr
	wg           sync.WaitGroup
	hostname     string // load on startup, cached

	subManager *subManager
}

func NewSubStore(closedConnCh <-chan string, debug bool) *subStore {
	if debug {
		sarama.Logger = l.New(os.Stdout, color.Blue("[Sarama]"),
			l.LstdFlags|l.Lshortfile)
	}

	return &subStore{
		hostname:     ctx.Hostname(),
		shutdownCh:   make(chan struct{}),
		closedConnCh: closedConnCh,
	}
}

func (this *subStore) Name() string {
	return "kafka"
}

func (this *subStore) Start() (err error) {
	this.subManager = newSubManager()

	this.wg.Add(1)
	go func() {
		defer this.wg.Done()

		var remoteAddr string
		for {
			select {
			case <-this.shutdownCh:
				log.Trace("sub store[%s] stopped", this.Name())
				return

			case remoteAddr = <-this.closedConnCh:
				this.wg.Add(1)
				go func(id string) {
					this.subManager.killClient(id)
					this.wg.Done()
				}(remoteAddr)
			}
		}
	}()

	return
}

func (this *subStore) Stop() {
	this.subManager.Stop()
	close(this.shutdownCh)
	this.wg.Wait()
}

func (this *subStore) Fetch(cluster, topic, group, remoteAddr, realIp,
	resetOffset string, permitStandby, mux bool) (store.Fetcher, error) {
	cg, err := this.subManager.PickConsumerGroup(cluster, topic, group, remoteAddr, realIp, resetOffset, permitStandby, mux)
	if err != nil {
		return nil, err
	}

	return &consumerFetcher{
		ConsumerGroup: cg,
		remoteAddr:    remoteAddr,
		store:         this,
	}, nil
}

func (this *subStore) IsSystemError(err error) bool {
	switch err {
	case consumergroup.ErrTooManyConsumers, store.ErrTooManyConsumers:
		return false

	default:
		if e, ok := err.(*sarama.ConsumerError); ok &&
			(e.Err == consumergroup.ErrInvalidTopic ||
				e.Err == consumergroup.ErrConsumerConflict ||
				e.Err == consumergroup.ErrTooManyConsumers) {
			return false
		}

		return true
	}
}
