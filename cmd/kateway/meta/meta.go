package meta

import (
	"time"

	"github.com/funkygao/gafka/zk"
)

type MetaStore interface {
	Start()
	Stop()
	RefreshInterval() time.Duration

	ZkCluster() *zk.ZkCluster

	Clusters() []map[string]string
	Partitions(topic string) []int32
	OnlineConsumersCount(topic, group string) int
	ZkAddrs() []string
	ZkChroot() string
	BrokerList() []string

	AuthPub(appid, pubkey, topic string) bool
	AuthSub(appid, subkey, topic string) bool
}

var Default MetaStore
