package mirror

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/kafka-cg/consumergroup"
)

func (this *Mirror) makePub(c2 *zk.ZkCluster) (sarama.AsyncProducer, error) {
	cf := sarama.NewConfig()
	cf.Metadata.RefreshFrequency = time.Minute * 10
	cf.Metadata.Retry.Max = 3
	cf.Metadata.Retry.Backoff = time.Second * 3

	cf.ChannelBufferSize = 1000

	cf.Producer.Flush.Frequency = time.Second // TODO
	cf.Producer.Flush.Messages = 2000
	cf.Producer.Flush.MaxMessages = 0 // unlimited
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Retry.Backoff = time.Second * 3
	cf.Producer.Retry.Max = 3

	switch this.Compress {
	case "gzip":
		cf.Producer.Compression = sarama.CompressionGZIP

	case "snappy":
		cf.Producer.Compression = sarama.CompressionSnappy
	}
	return sarama.NewAsyncProducer(c2.BrokerList(), cf)
}

func (this *Mirror) makeSub(c1 *zk.ZkCluster, group string, topics []string) (*consumergroup.ConsumerGroup, error) {
	cf := consumergroup.NewConfig()
	cf.Zookeeper.Chroot = c1.Chroot()
	cf.Offsets.CommitInterval = time.Second * 10
	cf.Offsets.ProcessingTimeout = time.Second
	cf.ChannelBufferSize = 100
	cf.Consumer.Return.Errors = true
	cf.OneToOne = false

	sub, err := consumergroup.JoinConsumerGroup(group, topics, c1.ZkZone().ZkAddrList(), cf)
	return sub, err
}
