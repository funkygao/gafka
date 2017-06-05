package command

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/bjtime"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
)

type clusterMessage struct {
	*sarama.ConsumerMessage
	cluster string
}

type Trace struct {
	Ui  cli.Ui
	Cmd string

	lastDuration time.Duration
	firstMsgCh   chan clusterMessage
	grep         string
}

var defaultTopicRetention = time.Hour * 24 * 7

func (this *Trace) Run(args []string) (exitCode int) {
	var (
		zone         string
		from         string
		highlight    bool
		pretty       bool
		since        string
		excludes     string
		echoFirstMsg bool
	)
	cmdFlags := flag.NewFlagSet("trace", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&from, "from", "", "")
	cmdFlags.DurationVar(&this.lastDuration, "last", time.Hour, "")
	cmdFlags.BoolVar(&highlight, "highlight", false, "")
	cmdFlags.StringVar(&this.grep, "grep", "", "")
	cmdFlags.StringVar(&since, "since", "", "")
	cmdFlags.StringVar(&excludes, "exclude", "", "")
	cmdFlags.BoolVar(&pretty, "pretty", false, "")
	cmdFlags.BoolVar(&echoFirstMsg, "checktime", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-from", "-grep").
		invalid(args) {
		return 2
	}

	if len(since) > 0 {
		bj, _ := time.LoadLocation("Asia/Shanghai")
		t, err := time.ParseInLocation("2006-01-02 15:04", since, bj)
		swallow(err)

		this.lastDuration = time.Since(t)
	}

	this.Ui.Infof("seeking %s %s ago", this.grep, this.lastDuration)

	msgChan := make(chan clusterMessage, 2000)
	this.firstMsgCh = make(chan clusterMessage, 100)
	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	for _, clusterTopic := range strings.Split(from, ",") {
		tuples := strings.SplitN(clusterTopic, "@", 2)
		if len(tuples) != 2 {
			panic(clusterTopic)
		}

		cluster, topic := tuples[1], tuples[0]
		zkcluster := zkzone.NewCluster(cluster)
		this.consumeCluster(zkcluster, topic, msgChan)
	}

	grepB := []byte(this.grep)
	excludedTopics := make(map[string]struct{})
	for _, t := range strings.Split(excludes, ",") {
		excludedTopics[t] = struct{}{}
	}
	var n int64
	progressInterval := time.Second * 30
	tick := time.NewTicker(progressInterval)
	var prettyJSON bytes.Buffer
	var content string
	for {
		select {
		case msg := <-this.firstMsgCh:
			if echoFirstMsg {
				this.Ui.Outputf("%s %s/%d@%s %s", color.Yellow("|"), msg.Topic, msg.Partition, msg.cluster, string(msg.Value))
			}

		case msg := <-msgChan:
			n++
			if bytes.Contains(msg.Value, grepB) {
				if _, present := excludedTopics[msg.Topic]; present {
					continue
				}

				progressInterval = time.Minute
				tick = time.NewTicker(progressInterval)

				if highlight {
					msg.Value = bytes.Replace(msg.Value, grepB, []byte(color.Red(this.grep)), -1)
				}

				if pretty {
					if err := json.Indent(&prettyJSON, msg.Value, "", "    "); err != nil {
						// FIXME when used with highlight, err: invalid character '\x1b' in string literal
						content = string(msg.Value)
					} else {
						content = string(prettyJSON.Bytes())
						prettyJSON.Reset()
					}
				} else {
					content = string(msg.Value)
				}

				this.Ui.Infof("%s/%d@%s", msg.Topic, msg.Partition, msg.cluster)
				this.Ui.Output(content)
			}

		case <-tick.C:
			this.Ui.Outputf("%16s msgs received, %s", gofmt.Comma(n), bjtime.TimeToString(time.Now()))
		}
	}

	return
}

func (this *Trace) consumeCluster(zkcluster *zk.ZkCluster, topic string, msgChan chan<- clusterMessage) {
	brokerList := zkcluster.BrokerList()
	if len(brokerList) == 0 {
		this.Ui.Warnf("cluster[%s] has no live brokers", zkcluster.Name())
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
		if t == topic {
			go this.consumeTopic(zkcluster, kfk, t, msgChan)
		}
	}
}

func (this *Trace) consumeTopic(zkcluster *zk.ZkCluster, kfk sarama.Client, topic string, msgCh chan<- clusterMessage) {
	consumer, err := sarama.NewConsumerFromClient(kfk)
	swallow(err)
	defer consumer.Close()

	partitions, err := kfk.Partitions(topic)
	swallow(err)

	for _, p := range partitions {
		var offset int64
		latestOffset, err := kfk.GetOffset(topic, p, sarama.OffsetNewest)
		swallow(err)

		oldestOffset, err := kfk.GetOffset(topic, p, sarama.OffsetOldest)
		swallow(err)

		retention := defaultTopicRetention
		cf, err := zkcluster.TopicConfigInfo(topic)
		if err == nil {
			retention = cf.RetentionSeconds()
		}
		if retention.Seconds() < 1 {
			retention = defaultTopicRetention
		}

		offset = latestOffset - (latestOffset-oldestOffset)*int64(this.lastDuration.Seconds())/int64(retention.Seconds())
		if offset <= 0 {
			this.Ui.Warnf("%s/%d empty", topic, p)
			continue
		}

		go this.consumePartition(zkcluster, kfk, consumer, topic, p, msgCh, offset)
	}
}

func (this *Trace) consumePartition(zkcluster *zk.ZkCluster, kfk sarama.Client, consumer sarama.Consumer,
	topic string, partitionId int32, msgCh chan<- clusterMessage, offset int64) {
	p, err := consumer.ConsumePartition(topic, partitionId, offset)
	if err != nil {
		this.Ui.Error(fmt.Sprintf("%s %s/%d: offset=%d %v", zkcluster.Name(), topic, partitionId, offset, err))
		return
	}
	defer p.Close()

	first := true
	for msg := range p.Messages() {
		msgCh <- clusterMessage{ConsumerMessage: msg, cluster: zkcluster.Name()}
		if first {
			first = false
			this.firstMsgCh <- clusterMessage{ConsumerMessage: msg, cluster: zkcluster.Name()}
		}
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

    -last duration
      Trace messages since last duration ago.
      Default 1h
      e,g.
      -last 5m
      -last 5h

    -since time
      e,g.
      -since '2006-01-02 15:04'

    -highlight

    -exclude comma seperated topic names

    -checktime
      Print out first message from each topics to validate the time range is ok.

    -pretty

`, this.Cmd, this.Synopsis(), ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
