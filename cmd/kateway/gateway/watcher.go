package gateway

import (
	"time"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
)

func (this *Gateway) watchDeadPartitions() {
	ticker := time.NewTicker(time.Minute * 2) // TODO
	defer ticker.Stop()

	var lastTopics = make(map[string]struct{})
	for {
		select {
		case <-ticker.C:
			deadPartitions := manager.Default.DeadPartitions()
			for topic, dp := range deadPartitions {
				store.DefaultPubStore.MarkPartitionsDead(topic, dp)

				lastTopics[topic] = struct{}{}
			}

			for lastDeadTopic, _ := range lastTopics {
				if _, present := deadPartitions[lastDeadTopic]; !present {
					// this topic was marked dead last round, but this round it comes alive
					store.DefaultPubStore.MarkPartitionsDead(lastDeadTopic, nil)
					delete(lastTopics, lastDeadTopic)
				}
			}

		case <-this.shutdownCh:
			return
		}
	}

}
