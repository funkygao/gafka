package main

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/pubsub/command"
	"github.com/funkygao/log4go"
)

// TODO how to load balance across the cluster?
// TODO how to init when startup, checkpoint of the routing table
func main() {
	level := log4go.DEBUG // TODO
	for _, filter := range log4go.Global {
		filter.Level = level
	}

	log4go.AddFilter("stdout", level, log4go.NewConsoleLogWriter())

	log4go.Info("waiting for new binding event")
	bindingChan := receiveNewBindings()
	for {
		select {
		case msg := <-bindingChan:
			log4go.Info("new binding: %s", msg.Value)
			addRouter(string(msg.Value))

		case <-time.After(time.Minute):
			log4go.Debug("no binding change during the last 1m")

		}
	}

}

// FIXME when to cleanup connections
func receiveNewBindings() <-chan *sarama.ConsumerMessage {
	kfk, err := sarama.NewClient(command.KafkaBrokerList, sarama.NewConfig())
	if err != nil {
		panic(err)
	}

	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		panic(err)
	}

	p, _ := consumer.ConsumePartition(command.BindingTopic, 0, sarama.OffsetNewest)
	return p.Messages()
}
