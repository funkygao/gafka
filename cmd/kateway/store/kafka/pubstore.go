package kafka

import (
	"fmt"
	l "log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/color"
	gio "github.com/funkygao/golib/io"
	log "github.com/funkygao/log4go"
)

type pubStore struct {
	shutdownCh chan struct{}

	wg       sync.WaitGroup
	hostname string // used as kafka client id
	dryRun   bool
	compress bool

	pubPools        map[string]*pubPool // key is cluster, each cluster maintains a conn pool
	pubPoolsCapcity int
	pubPoolsLock    sync.RWMutex
	idleTimeout     time.Duration

	// to avoid too frequent refresh
	// TODO refresh by cluster: current implementation will refresh zone
	lastRefreshedAt time.Time
}

func NewPubStore(poolCapcity int, idleTimeout time.Duration, compress bool,
	debug bool, dryRun bool) *pubStore {
	if debug {
		sarama.Logger = l.New(os.Stdout, color.Green("[Sarama]"), l.LstdFlags|l.Lshortfile)
	}

	return &pubStore{
		hostname:        ctx.Hostname(),
		compress:        compress,
		idleTimeout:     idleTimeout,
		pubPoolsCapcity: poolCapcity,
		pubPools:        make(map[string]*pubPool),
		dryRun:          dryRun,
		shutdownCh:      make(chan struct{}),
	}
}

func (this *pubStore) Name() string {
	return "kafka"
}

func (this *pubStore) Start() (err error) {
	if ctx.KafkaHome() == "" {
		return fmt.Errorf("empty kafka_home in ~/.gafka.cf")
	}
	if !gio.DirExists(ctx.KafkaHome()) {
		return fmt.Errorf("kafka not installed in %s, run 'gk deploy -kfkonly'", ctx.KafkaHome())
	}

	// warmup: create pools according the current kafka topology
	for _, cluster := range meta.Default.ClusterNames() {
		this.pubPools[cluster] = newPubPool(this, cluster,
			meta.Default.BrokerList(cluster), this.pubPoolsCapcity)
	}

	this.wg.Add(1)
	go func() {
		defer this.wg.Done()

		for {
			select {
			case <-meta.Default.RefreshEvent():
				this.doRefresh()

			case <-this.shutdownCh:
				log.Trace("pub store[%s] stopped", this.Name())
				return
			}
		}
	}()

	return
}

func (this *pubStore) Stop() {
	this.pubPoolsLock.Lock()
	defer this.pubPoolsLock.Unlock()

	// close all kafka connections
	for _, pool := range this.pubPools {
		pool.Close()
	}

	close(this.shutdownCh)
	this.wg.Wait()
}

func (this *pubStore) doRefresh() {
	if time.Since(this.lastRefreshedAt) <= time.Second*5 {
		log.Warn("ignored too frequent refresh: %s", time.Since(this.lastRefreshedAt))
		return
	}

	this.pubPoolsLock.Lock()
	defer this.pubPoolsLock.Unlock()

	// pub pool
	activeClusters := make(map[string]struct{})
	for _, cluster := range meta.Default.ClusterNames() {
		activeClusters[cluster] = struct{}{}
		if _, present := this.pubPools[cluster]; !present {
			// found a new cluster
			this.pubPools[cluster] = newPubPool(this, cluster,
				meta.Default.BrokerList(cluster), this.pubPoolsCapcity)
		} else {
			this.pubPools[cluster].RefreshBrokerList(meta.Default.BrokerList(cluster))
		}
	}

	// shutdown the dead clusters
	for cluster, pool := range this.pubPools {
		if _, present := activeClusters[cluster]; !present {
			// this cluster is dead or removed forever
			pool.Close()
			delete(this.pubPools, cluster)
		}
	}

	this.lastRefreshedAt = time.Now()
}

func (this *pubStore) markPartitionsDead(topic string, deadPartitionIds map[int32]struct{}) {
	excludedPartitionsLock.Lock()
	if deadPartitionIds == nil {
		// this topic comes alive
		delete(excludedPartitions, topic)
	} else {
		excludedPartitions[topic] = deadPartitionIds
	}
	excludedPartitionsLock.Unlock()
}

func (this *pubStore) watchDeadPartitions() {
	ticker := time.NewTicker(time.Minute * 2) // TODO
	defer ticker.Stop()

	var lastTopics = make(map[string]struct{})
	for {
		select {
		case <-ticker.C:
			deadPartitions := manager.Default.DeadPartitions()
			for topic, dp := range deadPartitions {
				this.markPartitionsDead(topic, dp)

				lastTopics[topic] = struct{}{}
			}

			for lastDeadTopic := range lastTopics {
				if _, present := deadPartitions[lastDeadTopic]; !present {
					// this topic was marked dead last round, but this round it comes alive

					log.Trace("%s come alive again", lastDeadTopic)

					this.markPartitionsDead(lastDeadTopic, nil)
					delete(lastTopics, lastDeadTopic)
				}
			}

		case <-this.shutdownCh:
			return
		}
	}

}
