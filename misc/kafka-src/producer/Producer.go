package main

import (
	"time"
)

type EventHandler interface {
	handle([]KeyedMessage)
	close()
}

type Producer struct {
	sync bool

	correlationId int64

	queue                        chan KeyedMessage
	eventHandler                 EventHandler
	topicMetadataRefreshInterval time.Duration
}

func NewProducer(config KafkaConfig) *Producer {
	this := &Producer{}
	switch config.producerType {
	case "sync":
		this.sync = true

	case "async":
		this.sync = false
		go this.producerSendThread()

	}

	return this
}

func (this *Producer) producerSendThread() {
	this.topicMetadataRefreshInterval = config.getDuration("topic.metadata.refresh.interval.ms",
		time.Minute*10)

	events := make([]KeyedMessage, 0)
	batchSize := config.getInt("batch.num.messages", 200)
	for msg := range this.queue {
		events = append(events, msg)
		if len(events) >= batchSize {
			this.tryHandle(events)

			events = make([]KeyedMessage, 0)
		}
	}
}

func (this *Producer) tryHandle(messages []KeyedMessage) {
	defer func() {
		recover()
	}()

	this.eventHandler.handle(messages)
}

func (this *Producer) send(messages []KeyedMessage) {
	switch this.sync {
	case true:
		this.eventHandler.handle(messages)

	case false:
		this.asyncSend(messages)
	}
}

func (this *Producer) asyncSend(messages []KeyedMessage) {
	for _, msg := range messages {
		this.queue <- msg
	}
}
