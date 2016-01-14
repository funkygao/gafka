package meta

import (
	"github.com/funkygao/gafka/zk"
)

// MetaStore is a generic storage that fetches meta data.
type MetaStore interface {
	Start()
	Stop()

	// RefreshEvent is fired whenever meta data is refreshed.
	RefreshEvent() <-chan struct{}

	ZkCluster(cluster string) *zk.ZkCluster

	// ClusterNames returns all live cluster names within the current zone.
	ClusterNames() []string

	// Clusters returns all live clusters name,nickname info.
	Clusters() []map[string]string

	TopicPartitions(cluster, topic string) []int32
	OnlineConsumersCount(cluster, topic, group string) int
	ZkAddrs() []string
	ZkChroot(cluster string) string

	// BrokerList returns the live brokers address list.
	BrokerList(cluster string) []string

	AuthPub(appid, pubkey, topic string) bool
	AuthSub(appid, subkey, topic string) bool
	LookupCluster(appid string) (string, bool)
}

var Default MetaStore
