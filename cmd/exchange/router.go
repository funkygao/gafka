package main

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/pubsub/command"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/log4go"
)

func addRouter(binding string) {
	fromTo := strings.SplitN(binding, ",", 2)
	from, to := fromTo[0], fromTo[1]
	p := strings.SplitN(from, ":", 2)
	fromApp, fromOutbox := p[0], p[1]
	p = strings.SplitN(to, ":", 2)
	toApp, toInbox := p[0], p[1]

	log4go.Info("add routing %s:%s -> %s:%s", fromApp, fromOutbox,
		toApp, toInbox)

	go runRouting(fromApp, fromOutbox, toApp, toInbox)
}

func runRouting(fromApp, fromOutbox, toApp, toInbox string) {
	fromTopic := command.KafkaOutboxTopic(fromApp, fromOutbox)
	toTopic := command.KafkaInboxTopic(toApp, toInbox)

	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Retry.Max = 3
	producer, err := sarama.NewSyncProducer(command.KafkaBrokerList, cf)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	kfk, err := sarama.NewClient(command.KafkaBrokerList, sarama.NewConfig())
	if err != nil {
		panic(err)
	}
	defer kfk.Close()
	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	p, _ := consumer.ConsumePartition(fromTopic, 0, sarama.OffsetNewest)
	defer p.Close()

	log4go.Info("router[%s:%s -> %s:%s] %s", fromApp, fromOutbox,
		toApp, toInbox,
		color.Green("ready"))

	var n int64 = 0
	for {
		select {
		case msg := <-p.Messages():
			stats.MsgPerSecond.Mark(1)

			n++
			if n%10000 == 0 {
				log4go.Debug("total %10s. [%s:%s -> %s:%s]: %s",
					gofmt.Comma(n),
					fromApp, fromOutbox,
					toApp, toInbox,
					string(msg.Value))
			}

			producer.SendMessage(&sarama.ProducerMessage{
				Topic: toTopic,
				Key:   sarama.StringEncoder(msg.Key),
				Value: sarama.StringEncoder(msg.Value),
			})

		}
	}

}
