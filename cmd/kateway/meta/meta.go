package meta

import (
	"time"

	"github.com/funkygao/gafka/zk"
)

type MetaStore interface {
	Start()
	Stop()
	RefreshInterval() time.Duration

	ZkCluster(cluster string) *zk.ZkCluster

	ClusterNames() []string
	Clusters() []map[string]string

	Partitions(cluster, topic string) []int32
	OnlineConsumersCount(cluster, topic, group string) int
	ZkAddrs() []string
	ZkChroot(cluster string) string
	BrokerList(cluster string) []string

	AuthPub(appid, pubkey, topic string) bool
	AuthSub(appid, subkey, topic string) bool

	LookupCluster(appid, topic string) string
}

var Default MetaStore
