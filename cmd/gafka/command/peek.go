package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/metrics"
)

var (
	stats *peekStats
)

type peekStats struct {
	MsgPerSecond metrics.Meter
}

func newPeekStats() *peekStats {
	this := &peekStats{
		MsgPerSecond: metrics.NewMeter(),
	}

	metrics.Register("msg.per.second", this.MsgPerSecond)
	return this
}

func (this *peekStats) start() {
	metrics.Log(metrics.DefaultRegistry, time.Second*10,
		log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
}

type Peek struct {
	Ui cli.Ui
}

func (this *Peek) Run(args []string) (exitCode int) {
	var (
		cluster     string
		zone        string
		topic       string
		partitionId int
		neat        bool
	)
	cmdFlags := flag.NewFlagSet("peek", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&topic, "t", "", "")
	cmdFlags.IntVar(&partitionId, "p", 0, "")
	cmdFlags.BoolVar(&neat, "n", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" || cluster == "" {
		this.Ui.Output("-z zone and -c cluster required")
		this.Ui.Output(this.Help())
		return 2
	}

	stats := newPeekStats()
	go stats.start()

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
	zkcluster := zkzone.NewCluster(cluster)
	brokerList := zkcluster.BrokerList()
	kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		this.Ui.Output(err.Error())
		return 1
	}
	defer kfk.Close()

	msgChan := make(chan string, 1000)
	if topic == "" {
		// peek all topics
		topics, err := kfk.Topics()
		if err != nil {
			this.Ui.Output(err.Error())
			return 1
		}

		for _, t := range topics {
			go this.consumeTopic(kfk, t, int32(partitionId), msgChan)
		}
	} else {
		go this.consumeTopic(kfk, topic, int32(partitionId), msgChan)
	}

	var msg string
	for {
		select {
		case msg = <-msgChan:
			stats.MsgPerSecond.Mark(1)

			if !neat {
				this.Ui.Output(msg)
			}
		}
	}

	return
}

func (this *Peek) consumeTopic(kfk sarama.Client, topic string, partitionId int32, msgCh chan string) {
	consumer, err := sarama.NewConsumerFromClient(kfk)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	p, _ := consumer.ConsumePartition(topic, partitionId, sarama.OffsetNewest)
	defer p.Close()

	for {
		select {
		case msg := <-p.Messages():
			msgCh <- fmt.Sprintf("%s %s %s", color.Green(topic),
				gofmt.Comma(msg.Offset), string(msg.Value))
		}
	}
}

func (*Peek) Synopsis() string {
	return "Peek kafka cluster messages ongoing"
}

func (*Peek) Help() string {
	help := `
Usage: gafka peek -z zone -c cluster [options]

	Peek kafka cluster messages ongoing

Options:

  -t topic name

  -p partition id

  -n
  	Neat mode, only display statastics instead of message content
`
	return strings.TrimSpace(help)
}
