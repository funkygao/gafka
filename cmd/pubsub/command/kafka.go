package command

import (
	"bufio"
	"fmt"

	"github.com/funkygao/golib/pipestream"
	log "github.com/funkygao/log4go"
)

const (
	kafkaHome = "/Users/apple/github/kafka-0.8.1.1-src" // TODO
)

var (
	kafkaBrokerList = []string{"localhost:9092"} // TODO
)

func KafkaCreateTopic(topic string) error {
	testcase := pipestream.New(fmt.Sprintf("%s/bin/kafka-topics.sh", kafkaHome),
		fmt.Sprintf("--zookeeper %s", ZkAddr),
		fmt.Sprintf("--create"),
		fmt.Sprintf("--topic %s", topic),
		fmt.Sprintf("--partitions 1"),         // TODO
		fmt.Sprintf("--replication-factor 1"), // TODO
	)
	err := testcase.Open()
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(testcase.Reader())
	scanner.Split(bufio.ScanLines)
	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
	}
	err = scanner.Err()
	if err != nil {
		return err
	}
	testcase.Close()

	log.Trace("%s", lastLine)

	return nil
}

func KafkaInboxTopic(app, inbox string) string {
	return fmt.Sprintf("_pubsub.%s.inbox.%s", app, inbox)
}

func KafkaOutboxTopic(app, outbox string) string {
	return fmt.Sprintf("_pubsub.%s.outbox.%s", app, outbox)
}
