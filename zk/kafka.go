package zk

import (
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
)

func (this *ZkCluster) SimpleConsumeKafkaTopic(topic string, msgChan chan<- *sarama.ConsumerMessage) error {
	if len(this.BrokerList()) == 0 {
		return errors.New("empty brokers")
	}

	kfk, err := sarama.NewClient(this.BrokerList(), sarama.NewConfig())
	if err != nil {
		return err
	}
	defer kfk.Close()

	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		return err
	}
	defer consumer.Close()

	partitions, err := kfk.Partitions(topic)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, p := range partitions {
		wg.Add(1)

		go this.consumePartition(consumer, topic, p, sarama.OffsetNewest, msgChan, &wg)
	}
	wg.Wait()
	return nil
}

func (this *ZkCluster) consumePartition(consumer sarama.Consumer, topic string, partitionID int32, offset int64, msgCh chan<- *sarama.ConsumerMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	p, err := consumer.ConsumePartition(topic, partitionID, offset)
	if err != nil {
		log.Error("cluster[%s] %s#%d: %v", this.name, topic, partitionID, err)
		return
	}
	defer p.Close()

	for {
		select {
		case err := <-p.Errors():
			log.Error("cluster[%s] %s#%d: %v", this.name, topic, partitionID, err)

		case msg := <-p.Messages():
			if msg != nil {
				msgCh <- msg
			}
		}

	}
}
