package main

import (
	"sync"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
)

type MetaStore interface {
	Start()
	Stop()
	Refresh()
	Clusters() []string
	Partitions(topic string) []int32
	ZkAddrs() string
	BrokerList() []string
	AuthPub(pubkey string) bool
	AuthSub(subkey string) bool
}

type zkMetaStore struct {
	brokerList    []string
	zkcluster     *zk.ZkCluster
	mu            sync.Mutex
	partitionsMap map[string][]int32
}

func newZkMetaStore(zone string, cluster string) MetaStore {
	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
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
}

func (this *zkMetaStore) Refresh() {
	this.brokerList = this.zkcluster.BrokerList()
	this.partitionsMap = make(map[string][]int32, len(this.partitionsMap))
}

func (this *zkMetaStore) Partitions(topic string) []int32 {
	if partitions, present := this.partitionsMap[topic]; present {
		return partitions
	}

	// cache miss
	this.mu.Lock()
	partitions := this.zkcluster.Partitions(topic)
	this.partitionsMap[topic] = partitions
	this.mu.Unlock()
	return partitions
}

func (this *zkMetaStore) BrokerList() []string {
	return this.brokerList
}

func (this *zkMetaStore) ZkAddrs() string {
	return this.zkcluster.ZkAddrs()
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
