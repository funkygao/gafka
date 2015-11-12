package command

import (
	"bufio"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/pipestream"
	log "github.com/funkygao/log4go"
)

const (
	kafkaHome = "/opt/app/kafka_2.10-0.8.1.1" // TODO
)

var (
	KafkaZkConnect  = ZkAddr + "/kafka_pubsub"
	KafkaBrokerList = []string{"localhost:9092"} // TODO
	BindingTopic    = "_bindings"
)

func KafkaCreateTopic(topic string) error {
	log.Debug("creating kafka topic: %s", topic)

	cmd := pipestream.New(fmt.Sprintf("%s/bin/kafka-topics.sh", kafkaHome),
		fmt.Sprintf("--zookeeper %s", KafkaZkConnect),
		fmt.Sprintf("--create"),
		fmt.Sprintf("--topic %s", topic),
		fmt.Sprintf("--partitions 1"),         // TODO
		fmt.Sprintf("--replication-factor 1"), // TODO
	)
	err := cmd.Open()
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
		log.Debug(lastLine)
	}
	err = scanner.Err()
	if err != nil {
		return err
	}
	cmd.Close()

	log.Info("kafka created topic[%s]: %s", topic, lastLine)

	return nil
}

func KafkaInboxTopic(app, inbox string) string {
	return fmt.Sprintf("_pubsub_%s_inbox_%s", app, inbox)
}

func KafkaOutboxTopic(app, outbox string) string {
	return fmt.Sprintf("_pubsub_%s_outbox_%s", app, outbox)
}

func KafkaNotifyBinding(msg string) error {
	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Retry.Max = 3
	producer, err := sarama.NewSyncProducer(KafkaBrokerList, cf)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: BindingTopic,
		Value: sarama.StringEncoder(msg),
	})

	return err
}
