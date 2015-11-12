package main

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/pubsub/command"
	"github.com/funkygao/log4go"
)

func addRouter(binding string) {
	fromTo := strings.SplitN(binding, ",", 2)
	from, to := fromTo[0], fromTo[1]
	p := strings.SplitN(from, ":", 2)
	fromApp, fromOutbox := p[0], p[1]
	p = strings.SplitN(to, ":", 2)
	toApp, toInbox := p[0], p[1]
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

	for {
		select {
		case msg := <-p.Messages():
			log4go.Debug("[%s->%s]: %s", fromTopic, toTopic, string(msg.Value))

			producer.SendMessage(&sarama.ProducerMessage{
				Topic: toTopic,
				Key:   sarama.StringEncoder(msg.Key),
				Value: sarama.StringEncoder(msg.Value),
			})

		}
	}

}
