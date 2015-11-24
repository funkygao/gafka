package main

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/pubsub/command"
	"github.com/funkygao/log4go"
)

func main() {
	level := log4go.DEBUG // TODO
	for _, filter := range log4go.Global {
		filter.Level = level
	}

	log4go.AddFilter("stdout", level, log4go.NewConsoleLogWriter())

	stats = newExchangeStats()
	stats.start()

	// FIXME race condition with receiveNewBindings, lost bindings
	loadBindings()

	log4go.Info("waiting for new binding event")
	bindingChan := receiveNewBindings()
	for {
		select {
		case msg := <-bindingChan:
			log4go.Info("received new binding event: %s", msg.Value)
			addRouter(string(msg.Value))

		case <-time.After(time.Minute):
			log4go.Debug("no binding change during the last 1m")

		}
	}

}

func loadBindings() {
	log4go.Debug("loading bindings")

	zk := command.NewZk(command.DefaultConfig("", command.ZkAddr))
	conn := zk.Conn()
	children, _, err := conn.Children(command.BindRoot)
	if err != nil {
		panic(command.BindRoot + ": " + err.Error())
	}

	for _, app := range children {
		log4go.Info("found bindings for: %s", app)

		bindings, err := zk.GetJsonData(command.BindRoot + "/" + app)
		if err != nil {
			log4go.Error(err)
			continue
		}
		for from, to := range bindings {
			addRouter(fmt.Sprintf("%s,%s:%s", from, app, to))
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
