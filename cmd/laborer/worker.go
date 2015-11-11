package main

import (
	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/pubsub/command"
)

func dispatchWorkers(zk *command.Zk) {
	// get all children data from command.BindRoot
	// consume from, then produce to

}

func carryPipe(from, to string) {
	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Retry.Max = 3
	producer, err := sarama.NewSyncProducer(command.KafkaBrokerList, cf)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	kfk, err := sarama.NewClient(kafkaBrokerList, sarama.NewConfig())
	if err != nil {
		panic(err)
	}
	defer kfk.Close()
	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	topic := KafkaInboxTopic(app, inbox)
	p, _ := consumer.ConsumePartition(command.KafkaInboxTopic(app, topic), 0, sarama.OffsetNewest)
	defer p.Close()

	var i int64 = 1
	for {
		select {
		case msg := <-p.Messages():
			i++
			if i%int64(step) == 0 {
				this.Ui.Output(color.Green("topic:%s consumed %d messages <- %s", topic,
					string(msg.Value)))
			}

			producer.SendMessage(msg)
		}
	}

}
