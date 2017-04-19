package command

import (
	"bytes"
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Trace struct {
	Ui  cli.Ui
	Cmd string

	lastDays int64 // peek the most recent N messages
	column   string
	grep     string
}

func (this *Trace) Run(args []string) (exitCode int) {
	var (
		zone string
		from string
	)
	cmdFlags := flag.NewFlagSet("trace", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&from, "from", "", "")
	cmdFlags.Int64Var(&this.lastDays, "last", 3, "")
	cmdFlags.StringVar(&this.grep, "grep", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-from", "-grep").
		invalid(args) {
		return 2
	}

	msgChan := make(chan *sarama.ConsumerMessage, 20000)
	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	for _, clusterTopic := range strings.Split(from, ",") {
		tuples := strings.SplitN(clusterTopic, "@", 2)
		if len(tuples) != 2 {
			panic(clusterTopic)
		}

		cluster, topic := tuples[0], tuples[1]
		zkcluster := zkzone.NewCluster(cluster)
		this.consumeCluster(zkcluster, topic, msgChan)
	}

	grepB := []byte(this.grep)
	for msg := range msgChan {
		if bytes.Contains(msg.Value, grepB) {
			this.Ui.Outputf("%20s %s", msg.Topic, string(msg.Value))
		}
	}

	return
}

func (this *Trace) consumeCluster(zkcluster *zk.ZkCluster, topicPattern string,
	msgChan chan *sarama.ConsumerMessage) {
	brokerList := zkcluster.BrokerList()
	if len(brokerList) == 0 {
		return
	}
	kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		this.Ui.Output(err.Error())
		return
	}
	//defer kfk.Close() // FIXME how to close it

	topics, err := kfk.Topics()
	if err != nil {
		this.Ui.Output(err.Error())
		return
	}

	for _, t := range topics {
		if patternMatched(t, topicPattern) {
			go this.consumeTopic(zkcluster, kfk, t, msgChan)
		}
	}
}

func (this *Trace) consumeTopic(zkcluster *zk.ZkCluster, kfk sarama.Client, topic string,
	msgCh chan *sarama.ConsumerMessage) {
	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// all partitions
	partitions, err := kfk.Partitions(topic)
	if err != nil {
		panic(err)
	}

	for _, p := range partitions {
		var offset int64
		latestOffset, err := kfk.GetOffset(topic, p, sarama.OffsetNewest)
		swallow(err)

		oldestOffset, err := kfk.GetOffset(topic, p, sarama.OffsetOldest)
		swallow(err)

		// most topics have retention of 7 days
		offset = oldestOffset + (latestOffset-oldestOffset)*this.lastDays/7
		if offset <= 0 {
			this.Ui.Warnf("%s/%d empty", topic, p)
			continue
		}

		go this.consumePartition(zkcluster, kfk, consumer, topic, p, msgCh, offset)
	}
}

func (this *Trace) consumePartition(zkcluster *zk.ZkCluster, kfk sarama.Client, consumer sarama.Consumer,
	topic string, partitionId int32, msgCh chan *sarama.ConsumerMessage, offset int64) {
	p, err := consumer.ConsumePartition(topic, partitionId, offset)
	if err != nil {
		this.Ui.Error(fmt.Sprintf("%s %s/%d: offset=%d %v", zkcluster.Name(), topic, partitionId, offset, err))
		return
	}
	defer p.Close()

	for msg := range p.Messages() {
		msgCh <- msg
	}
}

func (*Trace) Synopsis() string {
	return "Trace needles in haystack"
}

func (this *Trace) Help() string {
	help := fmt.Sprintf(`
Usage: %s trace [options]

    %s

Options:

    -z zone
      Default %s

    -from cluster@topic,cluster@topic,...
      e,g.
      -from logs@gateway,logstash@apache

    -grep pattern

    -last days
      Trace messages from last N days.
      Default 3

`, this.Cmd, this.Synopsis(), ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
