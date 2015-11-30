package main

import (
	"strings"
	"sync"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

type MetaStore interface {
	Start()
	Stop()
	Refresh()
	Clusters() []string
	Partitions(topic string) []int32
	OnlineConsumersCount(topic, group string) int
	ZkAddrs() []string
	ZkChroot() string
	BrokerList() []string
	AuthPub(pubkey string) bool
	AuthSub(subkey string) bool
}

type zkMetaStore struct {
	brokerList []string // cache

	partitionsMap map[string][]int32 // cache
	pmapLock      sync.RWMutex

	zkcluster *zk.ZkCluster
}

func newZkMetaStore(zone string, cluster string) MetaStore {
	zkAddrs := ctx.ZoneZkAddrs(zone)
	if len(zkAddrs) == 0 {
		panic("empty zookeeper addr")
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, zkAddrs))
	return &zkMetaStore{
		zkcluster:     zkzone.NewCluster(cluster),
		partitionsMap: make(map[string][]int32),
	}
}

func (this *zkMetaStore) Start() {
	this.brokerList = this.zkcluster.BrokerList()
}

func (this *zkMetaStore) Stop() {
	this.zkcluster.Close()
	log.Info("meta store closed")
}

func (this *zkMetaStore) OnlineConsumersCount(topic, group string) int {
	// without cache
	return this.zkcluster.OnlineConsumersCount(topic, group)
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
	this.zkcluster.ZkZone().WithinClusters(func(name, path string) {
		r = append(r, name)
	})
	return r
}

func (this *zkMetaStore) AuthPub(pubkey string) (ok bool) {
	return true
}

func (this *zkMetaStore) AuthSub(subkey string) (ok bool) {
	return true
}
