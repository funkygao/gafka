package zkmeta

import (
	"strings"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

type zkMetaStore struct {
	cf *config
	mu sync.RWMutex

	shutdownCh chan struct{}
	refreshCh  chan struct{} // deadlock if no receiver

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
		refreshCh:  make(chan struct{}, 5),

		brokerList:    make(map[string][]string),
		clusters:      make(map[string]*zk.ZkCluster),
		partitionsMap: make(map[string]map[string][]int32),
	}
}

func (this *zkMetaStore) Name() string {
	return "zk"
}

func (this *zkMetaStore) RefreshEvent() <-chan struct{} {
	return this.refreshCh
}

func (this *zkMetaStore) refreshTopologyCache() {
	// refresh live clusters from Zookeeper
	liveClusters := this.zkzone.Clusters()

	this.mu.Lock()

	// add new live clusters if not present in my cache
	for cluster, path := range liveClusters {
		if _, present := this.clusters[cluster]; !present {
			this.clusters[cluster] = this.zkzone.NewclusterWithPath(cluster, path)
		}

		this.brokerList[cluster] = this.clusters[cluster].BrokerList()
	}

	// remove dead clusters
	cachedClusters := this.clusters
	for cluster, _ := range cachedClusters {
		if _, present := liveClusters[cluster]; !present {
			delete(this.clusters, cluster)
			delete(this.brokerList, cluster)
		}
	}

	this.mu.Unlock()
}

func (this *zkMetaStore) Start() {
	// warm up
	this.refreshTopologyCache()

	go func() {
		ticker := time.NewTicker(this.cf.Refresh)
		defer ticker.Stop()

		booting := true
		for {
			select {
			case <-ticker.C:
				log.Debug("refreshing zk meta store")

				this.refreshTopologyCache()

				// clear the partition cache
				this.pmapLock.Lock()
				this.partitionsMap = make(map[string]map[string][]int32,
					len(this.partitionsMap))
				this.pmapLock.Unlock()

				// notify others that I have got the most recent data
				this.refreshCh <- struct{}{}

			case <-this.shutdownCh:
				return

			case evt := <-this.zkzone.SessionEvents():
				// after zk conn lost, zklib will automatically reconnect:
				// StateConnecting -> StateConnected -> StateHasSession
				if evt.State == zklib.StateHasSession {
					if booting {
						booting = false
					} else {
						log.Warn("zk reconnected after session lost")
					}
				}
			}
		}
	}()
}

func (this *zkMetaStore) Stop() {
	this.mu.Lock()
	defer this.mu.Unlock()

	for _, c := range this.clusters {
		c.Close()
	}

	close(this.shutdownCh)
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

	// FIXME will always lookup zk
	return c.OnlineConsumersCount(topic, group)
}

func (this *zkMetaStore) TopicPartitions(cluster, topic string) []int32 {
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
	defer this.mu.RUnlock()

	this.zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		info := zkcluster.RegisteredInfo()
		if !info.Public || info.Nickname == "" {
			// ignored for kateway manager
			return
		}

		c := make(map[string]string)
		c["name"] = info.Name()
		c["nickname"] = info.Nickname
		r = append(r, c)
	})
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
