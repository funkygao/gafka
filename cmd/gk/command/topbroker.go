package command

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type TopBroker struct {
	Ui  cli.Ui
	Cmd string

	zone, cluster, topic string
	drawMode             bool
	interval             time.Duration

	offsets     map[string]int64 // host => offset sum
	lastOffsets map[string]int64
}

func (this *TopBroker) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("topbroker", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.topic, "t", "", "")
	cmdFlags.BoolVar(&this.drawMode, "g", false, "")
	cmdFlags.DurationVar(&this.interval, "i", time.Second*5, "refresh interval")

	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.offsets = make(map[string]int64)
	this.lastOffsets = make(map[string]int64)

	if this.interval.Seconds() < 1 {
		this.interval = time.Second
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), this.cluster) {
			return
		}

		go this.clusterTopProducers(zkcluster)
	})

	ticker := time.NewTicker(this.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			refreshScreen()
			this.showAndResetCounters()

		}
	}

	return
}

func (this *TopBroker) showAndResetCounters() {
	d := this.interval.Seconds()
	for host, offset := range this.offsets {
		qps := float64(0)
		if lastOffset, present := this.lastOffsets[host]; present {
			qps = float64(offset-lastOffset) / d
		}

		this.Ui.Output(fmt.Sprintf("%20s %.2f", host, qps))
	}

	for host, offset := range this.offsets {
		this.lastOffsets[host] = offset
	}
	this.offsets = make(map[string]int64)
}

func (this *TopBroker) clusterTopProducers(zkcluster *zk.ZkCluster) {
	kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		return
	}
	defer kfk.Close()

	for {
		topics, err := kfk.Topics()
		swallow(err)

		for _, topic := range topics {
			if !patternMatched(topic, this.topic) {
				continue
			}

			partions, err := kfk.WritablePartitions(topic)
			swallow(err)
			for _, partitionID := range partions {
				leader, err := kfk.Leader(topic, partitionID)
				swallow(err)

				latestOffset, err := kfk.GetOffset(topic, partitionID,
					sarama.OffsetNewest)
				if err != nil {
					panic(err)
				}

				host, _, err := net.SplitHostPort(leader.Addr())
				swallow(err)

				host = shortIp(host)
				if _, present := this.offsets[host]; !present {
					this.offsets[host] = 0
				}
				this.offsets[host] += latestOffset
			}
		}

		time.Sleep(time.Second)
		kfk.RefreshMetadata(topics...)
	}
}

func (*TopBroker) Synopsis() string {
	return "Unix “top” like utility for kafka brokers"
}

func (this *TopBroker) Help() string {
	help := fmt.Sprintf(`
Usage: %s topbroker [options]

    Unix “top” like utility for kafka brokers

Options:

    -z zone
      Default %s

    -c cluster pattern

    -t topic pattern  

    -i interval
      Refresh interval in seconds.
      e,g. 5s    

`, this.Cmd)
	return strings.TrimSpace(help)
}
