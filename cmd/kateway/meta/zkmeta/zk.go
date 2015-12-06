package zkmeta

import (
	"strings"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

type zkMetaStore struct {
	shutdownCh chan struct{}
	refresh    time.Duration

	brokerList []string // cache

	partitionsMap map[string][]int32 // cache
	pmapLock      sync.RWMutex

	zkcluster *zk.ZkCluster
}

func NewZkMetaStore(zone string, cluster string, refresh time.Duration) meta.MetaStore {
	zkAddrs := ctx.ZoneZkAddrs(zone)
	if len(zkAddrs) == 0 {
		panic("empty zookeeper addr")
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, zkAddrs)) // TODO session timeout
	return &zkMetaStore{
		zkcluster:     zkzone.NewCluster(cluster),
		partitionsMap: make(map[string][]int32),
		refresh:       refresh,
		shutdownCh:    make(chan struct{}),
	}
}

func (this *zkMetaStore) Start() {
	this.brokerList = this.zkcluster.BrokerList()

	go func() {
		ticker := time.NewTicker(this.refresh)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				this.Refresh()

			case <-this.shutdownCh:
				log.Trace("meta store closed")
				return
			}
		}
	}()
}

func (this *zkMetaStore) Stop() {
	this.zkcluster.Close()
	close(this.shutdownCh)

}

func (this *zkMetaStore) OnlineConsumersCount(topic, group string) int {
	// without cache
	return this.zkcluster.OnlineConsumersCount(topic, group)
}

func (this *zkMetaStore) RefreshInterval() time.Duration {
	return this.refresh
}

func (this *zkMetaStore) Refresh() {
	this.brokerList = this.zkcluster.BrokerList()

	this.pmapLock.Lock()
	this.partitionsMap = make(map[string][]int32, len(this.partitionsMap))
	this.pmapLock.Unlock()
}

func (this *zkMetaStore) Partitions(topic string) []int32 {
	this.pmapLock.RLock()
	partitions, present := this.partitionsMap[topic]
	this.pmapLock.RUnlock()
	if present {
		return partitions
	}

	// cache miss
	partitions = this.zkcluster.Partitions(topic)
	this.pmapLock.Lock()
	this.partitionsMap[topic] = partitions
	this.pmapLock.Unlock()
	return partitions
}

func (this *zkMetaStore) BrokerList() []string {
	return this.brokerList
}

func (this *zkMetaStore) ZkAddrs() []string {
	return strings.Split(this.zkcluster.ZkZone().ZkAddrs(), ",")
}

func (this *zkMetaStore) ZkChroot() string {
	return this.zkcluster.Chroot()
}

func (this *zkMetaStore) Clusters() []string {
	r := make([]string, 0)
	this.zkcluster.ZkZone().ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		r = append(r, zkcluster.Name())
	})
	return r
}

func (this *zkMetaStore) ZkCluster() *zk.ZkCluster {
	return this.zkcluster
}

func (this *zkMetaStore) AuthPub(appid, pubkey, topic string) (ok bool) {
	return true
}

func (this *zkMetaStore) AuthSub(appid, subkey, topic string) (ok bool) {
	return true
}
