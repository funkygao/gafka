package gateway

import (
	"time"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
)

func (this *Gateway) watchDeadPartitions() {
	ticker := time.NewTicker(time.Minute * 2) // TODO
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for topic, dp := range manager.Default.DeadPartitions() {
				store.DefaultPubStore.MarkPartitionsDead(topic, dp)
			}

		case <-this.shutdownCh:
			return
		}
	}

}
