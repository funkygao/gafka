package zkmeta

import (
	"strings"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/structs"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

type zkMetaStore struct {
	cf *config
	mu sync.RWMutex
	wg sync.WaitGroup

	shutdownCh chan struct{}
	refreshCh  chan struct{}

	zkzone *zk.ZkZone

	// cache
	brokerList map[string][]string      // key is cluster name
	clusters   map[string]*zk.ZkCluster // key is cluster name

	// cache
	partitionsMap map[structs.ClusterTopic][]int32
	pmapLock      sync.RWMutex
}

func New(cf *config, zkzone *zk.ZkZone) meta.MetaStore {
	return &zkMetaStore{
		cf:         cf,
		zkzone:     zkzone,
		shutdownCh: make(chan struct{}),
		refreshCh:  make(chan struct{}, 5),

		brokerList:    make(map[string][]string),
		clusters:      make(map[string]*zk.ZkCluster),
		partitionsMap: make(map[structs.ClusterTopic][]int32),
	}
}

func (this *zkMetaStore) Name() string {
	return "zk"
}

func (this *zkMetaStore) RefreshEvent() <-chan struct{} {
	return this.refreshCh
}

func (this *zkMetaStore) refreshTopologyCache() error {
	liveClusters := this.zkzone.Clusters()
	if len(liveClusters) == 0 {
		return ErrZkBroken
	}

	this.mu.Lock()
	defer this.mu.Unlock()

	// add new live clusters if not present in my cache
	for cluster, path := range liveClusters {
		if _, present := this.clusters[cluster]; !present {
			this.clusters[cluster] = this.zkzone.NewclusterWithPath(cluster, path)
		}

		brokerList := this.clusters[cluster].BrokerList()
		if len(brokerList) == 0 {
			return ErrZkBroken
		}
		this.brokerList[cluster] = brokerList
	}

	// remove dead clusters
	cachedClusters := this.clusters
	for cluster := range cachedClusters {
		if _, present := liveClusters[cluster]; !present {
			delete(this.clusters, cluster)
			delete(this.brokerList, cluster)
		}
	}

	return nil
}

func (this *zkMetaStore) Start() {
	// warm up
	this.refreshTopologyCache()

	this.wg.Add(1)
	go func() {
		ticker := time.NewTicker(this.cf.Refresh)
		defer func() {
			ticker.Stop()
			this.wg.Done()
		}()

		for {
			select {
			case <-ticker.C:
				log.Debug("refreshing zk meta store")

				if err := this.refreshTopologyCache(); err != nil {
					// zk connection broken, refuse to refresh cache
					/// next tick, zk connection might be fixed
					log.Warn("meta refresh: %s", err)
					continue
				}

				// clear the partition cache
				this.pmapLock.Lock()
				this.partitionsMap = make(map[structs.ClusterTopic][]int32, len(this.partitionsMap))
				this.pmapLock.Unlock()

				// notify others that I have got the most recent data
				select {
				case this.refreshCh <- struct{}{}:
				default:
				}

			case <-this.shutdownCh:
				return
			}
		}
	}()
}

func (this *zkMetaStore) Stop() {
	this.mu.Lock()
	defer this.mu.Unlock()

	close(this.shutdownCh)
	this.wg.Wait()
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
	ct := structs.ClusterTopic{Cluster: cluster, Topic: topic}

	this.pmapLock.RLock()
	if partitionIDs, present := this.partitionsMap[ct]; present {
		this.pmapLock.RUnlock()
		return partitionIDs
	}
	this.pmapLock.RUnlock()

	this.pmapLock.Lock()
	defer this.pmapLock.Unlock()

	// double check
	if partitionIDs, present := this.partitionsMap[ct]; present {
		return partitionIDs
	}

	// cache miss
	this.mu.RLock()
	c, ok := this.clusters[cluster]
	this.mu.RUnlock()
	if !ok {
		log.Warn("invalid cluster: %s", cluster)
		return nil
	}

	partitionIDs := c.Partitions(topic)
	// set cache
	this.partitionsMap[ct] = partitionIDs

	return partitionIDs
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
	}

	return ""
}

func (this *zkMetaStore) ClusterNames() []string {
	this.mu.RLock()
	r := make([]string, 0, len(this.clusters))
	for name := range this.clusters {
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
