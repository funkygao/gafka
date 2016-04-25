package main

import (
	"math/rand"
)

type BrokerPartitionInfo struct {
	brokerList         []string
	topicPartitionInfo map[string]TopicMetadata // cache
}

func (this *BrokerPartitionInfo) getBrokerPartitionInfo(topic string,
	correlationId int64) []PartitionAndLeader {
	if _, present := this.topicPartitionInfo[topic]; !present {
		// refresh the topic metadata cache
		this.updateInfo([]string{topic}, correlationId)
		if _, present := this.topicPartitionInfo[topic]; !present {
			panic("Failed to fetch topic metadata for topic:" + topic)
		}
	}
}

// updates the cache by issuing a get topic metadata request to a random broker
func (this *BrokerPartitionInfo) updateInfo(topics []string, correlationId int64) {
	req := TopicMetadataRequest{}
	shuffledBrokers := rand.Shuffle(this.brokerList)
	fetchMetaDataSucceeded := false
	for i := 0; i < len(shuffledBrokers); i++ {
		topicMetadataResponse := sendToBroker(req, shuffledBrokers[i])
		if fetchMetaDataSucceeded {
			break
		}
	}
}
