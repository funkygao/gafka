package zkmeta

import (
	"github.com/funkygao/gafka/zk"
)

// Distribution of underly store clusters to multi-tenants.
func (this *zkMetaStore) AssignClusters() []map[string]string {
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
