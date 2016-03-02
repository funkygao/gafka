package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
)

var (
	stats *peekStats
)

type peekStats struct {
	MsgCountPerSecond metrics.Meter
	MsgBytesPerSecond metrics.Meter
}

func newPeekStats() *peekStats {
	this := &peekStats{
		MsgCountPerSecond: metrics.NewMeter(),
		MsgBytesPerSecond: metrics.NewMeter(),
	}

	metrics.Register("msg.count.per.second", this.MsgCountPerSecond)
	metrics.Register("msg.bytes.per.second", this.MsgBytesPerSecond)
	return this
}

func (this *peekStats) start() {
	metrics.Log(metrics.DefaultRegistry, time.Second*10,
		log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
}

type Peek struct {
	Ui  cli.Ui
	Cmd string

	offset   int64
	colorize bool
	limit    int
}

func (this *Peek) Run(args []string) (exitCode int) {
	var (
		cluster      string
		zone         string
		topicPattern string
		partitionId  int
		silence      bool
	)
	cmdFlags := flag.NewFlagSet("peek", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&topicPattern, "t", "", "")
	cmdFlags.IntVar(&partitionId, "p", 0, "")
	cmdFlags.BoolVar(&this.colorize, "color", true, "")
	cmdFlags.IntVar(&this.limit, "limit", -1, "")
	cmdFlags.Int64Var(&this.offset, "offset", sarama.OffsetNewest, "")
	cmdFlags.BoolVar(&silence, "s", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if silence {
		stats := newPeekStats()
		go stats.start()
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	msgChan := make(chan *sarama.ConsumerMessage, 20000) // msg aggerator channel
	if cluster == "" {
		zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
			this.consumeCluster(zkcluster, topicPattern, partitionId, msgChan)
		})
	} else {
		zkcluster := zkzone.NewCluster(cluster)
		this.consumeCluster(zkcluster, topicPattern, partitionId, msgChan)
	}

	var (
		msg   *sarama.ConsumerMessage
		total int
	)
LOOP:
	for {
		select {
		case msg = <-msgChan:
			if silence {
				stats.MsgCountPerSecond.Mark(1)
				stats.MsgBytesPerSecond.Mark(int64(len(msg.Value)))
			} else {
				if this.colorize {
					this.Ui.Output(fmt.Sprintf("%s/%d %s k:%s, v:%s",
						color.Green(msg.Topic), msg.Partition,
						gofmt.Comma(msg.Offset), string(msg.Key), string(msg.Value)))
				} else {
					// colored UI will have invisible chars output
					fmt.Println(fmt.Sprintf("%s/%d %s k:%s, v:%s",
						msg.Topic, msg.Partition,
						gofmt.Comma(msg.Offset), string(msg.Key), string(msg.Value)))
				}
			}

			if this.limit > 0 {
				total++
				if total >= this.limit {
					break LOOP
				}
			}

		}
	}

	return
}

func (this *Peek) consumeCluster(zkcluster *zk.ZkCluster, topicPattern string,
	partitionId int, msgChan chan *sarama.ConsumerMessage) {
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
			go this.simpleConsumeTopic(kfk, t, int32(partitionId), msgChan)
		}
	}

}

func (this *Peek) simpleConsumeTopic(kfk sarama.Client, topic string, partitionId int32,
	msgCh chan *sarama.ConsumerMessage) {
	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	if partitionId == -1 {
		// all partitions
		partitions, err := kfk.Partitions(topic)
		if err != nil {
			panic(err)
		}

		for _, p := range partitions {
			go this.consumePartition(kfk, consumer, topic, p, msgCh)
		}

	} else {
		this.consumePartition(kfk, consumer, topic, partitionId, msgCh)
	}

}

func (this *Peek) consumePartition(kfk sarama.Client, consumer sarama.Consumer,
	topic string, partitionId int32, msgCh chan *sarama.ConsumerMessage) {
	p, err := consumer.ConsumePartition(topic, partitionId, this.offset)
	if err != nil {
		this.Ui.Error(fmt.Sprintf("%s/%d: %v", topic, partitionId, err))
		os.Exit(1)
	}
	defer p.Close()

	for {
		select {
		case msg := <-p.Messages():
			msgCh <- msg
		}
	}
}

func (*Peek) Synopsis() string {
	return "Peek kafka cluster messages ongoing from any offset"
}

func (this *Peek) Help() string {
	help := fmt.Sprintf(`
Usage: %s peek [options]

    Peek kafka cluster messages ongoing from any offset

Options:

    -z zone
      Default %s

    -c cluster

    -t topic pattern
    
    -p partition id
      -1 will peek all partitions of a topic

    -offset message offset value
      -1 OffsetNewest, -2 OffsetOldest. 
      You can specify your own offset.
      Default -1(OffsetNewest)

    -limit n
      Limit how many messages to consume

    -s
      Silence mode, only display statastics instead of message content

    -color
      Enable colorized output
`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
