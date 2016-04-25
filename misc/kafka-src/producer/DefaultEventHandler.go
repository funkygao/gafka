package main

import (
	"log"
	"sync/atomic"
	"time"
)

type EventHandler interface {
	handle([]KeyedMessage)
	close()
}

type DefaultEventHandler struct {
	topicMetadataRefreshInterval time.Duration       // 10m by default
	brokerPartitionInfo          BrokerPartitionInfo // local meta data info
	producerPool                 map[int]SyncProducer
}

func (this *DefaultEventHandler) handle(messages []KeyedMessage) {
	serializedData := serialize(messages)
	remainingRetries := config.getInt("message.send.max.retries", 3)
	outstandingProduceRequests := serializedData
	for i := 0; i < remainingRetries; i++ {
		if len(outstandingProduceRequests) == 0 {
			break
		}

		if shouldRefreshTopicMeta {
			atomic.AddInt64(&corelationId, 1)
			this.brokerPartitionInfo.updateInfo(topics, correlationId)
		}

		// the underlying action
		outstandingProduceRequests = this.dispatchSerializedData(outstandingProduceRequests)
		if len(outstandingProduceRequests) > 0 {
			log.Println("Back off for %d ms before retrying send.")
			time.Sleep(config.getDuration("retry.backoff.ms", time.Millisecond*100))

			atomic.AddInt64(&corelationId, 1)
			this.brokerPartitionInfo.updateInfo(outstandingProduceRequests.topics(), correlationId)
		}
	}

	if len(outstandingProduceRequests) > 0 {
		log.Println("Failed to send messages after 3 retries")
	}
}

func (this *DefaultEventHandler) dispatchSerializedData(messages []KeyedMessage) []KeyedMessage {
	partitionedData := this.partitionAndCollate(messages)
	failedProduceRequests := make([]KeyedMessage, 0)
	for brokerId, messagesPerBrokerMap := range partitionedData {
		messageSetPerBroker := groupMessagesToSet(messagesPerBrokerMap)
		failedTopicPartitions := this.send(brokerid, messageSetPerBroker)
		for _, ftp := range failedTopicPartitions {
			failedProduceRequests = append(failedProduceRequests, ftp)
		}
	}

	return failedProduceRequests
}

func (this *DefaultEventHandler) partitionAndCollate(messages []KeyedMessage) map[int]map[TopicAndPartition][]KeyedMessage {
	// return brokerId =>
	//			  TopicAndPartition =>
	//					[]KeyedMessage
}

func (this *DefaultEventHandler) send(brokerId int, messagesPerTopic map[TopicAndPartition][]KeyedMessage) []TopicAndPartition {
	failedTopicPartitions := make([]TopicAndPartition, 0)
	producerRequest := ProducerRequest{}
	syncProducer := producerPool.getProducer(brokerId)
	response := syncProducer.send(producerRequest)
	// calculate failed topic partition according to the response
	return failedTopicPartitions
}
