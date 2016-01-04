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
	cf         *config
	shutdownCh chan struct{}
	mu         sync.RWMutex

	zkzone *zk.ZkZone

	// cache
	brokerList map[string][]string      // key is cluster name
	clusters   map[string]*zk.ZkCluster // key is cluster name

	// cache
	partitionsMap map[string]map[string][]int32 // {cluster: {topic: partitions}}
	pmapLock      sync.RWMutex
}

func New(cf *config) meta.MetaStore {
	if cf.Zone == "" {
		panic("empty zone")
	}
	zkAddrs := ctx.ZoneZkAddrs(cf.Zone)
	if len(zkAddrs) == 0 {
		panic("empty zookeeper addr")
	}

	return &zkMetaStore{
		cf:         cf,
		zkzone:     zk.NewZkZone(zk.DefaultConfig(cf.Zone, zkAddrs)), // TODO session timeout
		shutdownCh: make(chan struct{}),

		brokerList:    make(map[string][]string),
		clusters:      make(map[string]*zk.ZkCluster),
		partitionsMap: make(map[string]map[string][]int32),
	}
}

func (this *zkMetaStore) RefreshInterval() time.Duration {
	return this.cf.Refresh
}

func (this *zkMetaStore) fillBrokerList() {
	this.mu.Lock()
	// zkzone.Clusters() will never cache
	for cluster, path := range this.zkzone.Clusters() {
		// FIXME don't work when cluster is deleted
		if _, present := this.clusters[cluster]; !present {
			this.clusters[cluster] = this.zkzone.NewclusterWithPath(cluster, path)
		}

		this.brokerList[cluster] = this.clusters[cluster].BrokerList()
	}

	this.mu.Unlock()
}

func (this *zkMetaStore) Start() {
	this.fillBrokerList()

	go func() {
		ticker := time.NewTicker(this.cf.Refresh)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				this.doRefresh()

			case <-this.shutdownCh:
				log.Trace("meta store closed")
				return
			}
		}
	}()
}

func (this *zkMetaStore) Stop() {
	this.mu.Lock()
	for _, c := range this.clusters {
		c.Close()
	}
	this.mu.Unlock()

	close(this.shutdownCh)
}

func (this *zkMetaStore) Refresh() {
	this.doRefresh()
}

func (this *zkMetaStore) OnlineConsumersCount(cluster, topic, group string) int {
	// without cache
	this.mu.Lock()
	c, present := this.clusters[cluster]
	this.mu.Unlock()

	if !present {
		log.Warn("invalid cluster: %s", cluster)
		return 0
	}

	return c.OnlineConsumersCount(topic, group)
}

func (this *zkMetaStore) doRefresh() {
	log.Trace("refreshing meta")

	this.fillBrokerList()

	// clear the partition cache
	this.pmapLock.Lock()
	this.partitionsMap = make(map[string]map[string][]int32, len(this.partitionsMap))
	this.pmapLock.Unlock()
}

func (this *zkMetaStore) Partitions(cluster, topic string) []int32 {
	clusterNotPresent := true

	this.pmapLock.RLock()
	if c, present := this.partitionsMap[cluster]; present {
		clusterNotPresent = false
		if p, present := c[topic]; present {
			this.pmapLock.RUnlock()
			return p
		}
	}
	this.pmapLock.RUnlock()

	// cache miss
	this.mu.RLock()
	c, ok := this.clusters[cluster]
	this.mu.RUnlock()
	if !ok {
		log.Warn("invalid cluster: %s", cluster)
		return nil
	}

	p := c.Partitions(topic)

	this.pmapLock.Lock()
	if clusterNotPresent {
		this.partitionsMap[cluster] = make(map[string][]int32)
	}
	this.partitionsMap[cluster][topic] = p
	this.pmapLock.Unlock()

	return p
}

func (this *zkMetaStore) BrokerList(cluster string) []string {
	this.mu.RLock()
	r := this.brokerList[cluster]
	this.mu.RUnlock()
	return r
}

func (this *zkMetaStore) ZkAddrs() []string {
	return strings.Split(this.zkzone.ZkAddrs(), ",")
}

func (this *zkMetaStore) ZkChroot(cluster string) string {
	this.mu.RLock()
	c, ok := this.clusters[cluster]
	this.mu.RUnlock()

	if ok {
		return c.Chroot()
	} else {
		return ""
	}
}

func (this *zkMetaStore) Clusters() []map[string]string {
	r := make([]map[string]string, 0)
	this.mu.RLock()
	this.zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		info := zkcluster.RegisteredInfo()
		c := make(map[string]string)
		c["name"] = info.Name()
		c["nickname"] = info.Nickname
		r = append(r, c)
	})
	this.mu.RUnlock()
	return r
}

func (this *zkMetaStore) ClusterNames() []string {
	this.mu.RLock()
	r := make([]string, 0, len(this.clusters))
	for name, _ := range this.clusters {
		r = append(r, name)
	}
	this.mu.RUnlock()
	return r
}

func (this *zkMetaStore) ZkCluster(cluster string) *zk.ZkCluster {
	this.mu.RLock()
	r, ok := this.clusters[cluster]
	this.mu.RUnlock()

	if !ok {
		log.Warn("invalid cluster: %s", cluster)
		return nil
	}

	return r
}

func (this *zkMetaStore) AuthPub(appid, pubkey, topic string) (ok bool) {
	return true
}

func (this *zkMetaStore) AuthSub(appid, subkey, topic string) (ok bool) {
	return true
}

func (this *zkMetaStore) LookupCluster(appid, topic string) (cluster string) {
	return "psub" // TODO
}
