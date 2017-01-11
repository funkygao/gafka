// Package meta manages the global topology information.
package meta

import (
	"github.com/funkygao/gafka/zk"
)

// MetaStore is a generic storage that fetches topology meta data.
type MetaStore interface {
	Name() string

	Start()
	Stop()

	// RefreshEvent is fired whenever meta data is refreshed.
	RefreshEvent() <-chan struct{}

	ZkCluster(cluster string) *zk.ZkCluster

	// ClusterNames returns all live cluster names within the current zone.
	ClusterNames() []string

	// AssignClusters is director of cluster distribution.
	AssignClusters() []map[string]string

	ZkAddrs() []string
	ZkChroot(cluster string) string

	// BrokerList returns the live brokers address list.
	BrokerList(cluster string) []string
}

var Default MetaStore
