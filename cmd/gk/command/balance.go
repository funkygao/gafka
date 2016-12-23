package command

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/structs"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/ryanuber/columnize"
)

type hostLoadInfo struct {
	host            string
	cluster         string
	topicPartitions []structs.TopicPartition
	qps             int64
}

type hostOffsetInfo struct {
	host      string
	offsetMap map[string]map[structs.TopicPartition]int64 // cluster:tp:offset
}

func (ho hostOffsetInfo) Total() (t int64) {
	for _, tps := range ho.offsetMap {
		for _, off := range tps {
			t += off
		}
	}
	return
}

type Balance struct {
	Ui  cli.Ui
	Cmd string

	zone, cluster string
	interval      time.Duration

	offsets     map[string]int64 // host => offset sum TODO
	lastOffsets map[string]int64

	lastHostOffsets map[string]hostOffsetInfo

	hostOffsetCh chan map[string]hostOffsetInfo // key is host
	signalsCh    map[string]chan struct{}
}

func (this *Balance) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("balance", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.DurationVar(&this.interval, "i", time.Second*5, "refresh interval")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.signalsCh = make(map[string]chan struct{})
	this.hostOffsetCh = make(chan map[string]hostOffsetInfo)

	this.lastHostOffsets = make(map[string]hostOffsetInfo)
	this.offsets = make(map[string]int64)
	this.lastOffsets = make(map[string]int64)

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), this.cluster) {
			return
		}

		this.signalsCh[zkcluster.Name()] = make(chan struct{})

		go this.clusterTopProducers(zkcluster)
	})

	this.drawSummary()

	return
}

func (this *Balance) startAll() {
	for _, ch := range this.signalsCh {
		ch <- struct{}{}
	}
}

func (this *Balance) collectAll(seq int) {
	for _, _ = range this.signalsCh {
		offsets := <-this.hostOffsetCh
		if seq == 0 {
			// record into lastHostOffsets
			for host, offsetInfo := range offsets {
				if _, present := this.lastHostOffsets[host]; !present {
					this.lastHostOffsets[host] = hostOffsetInfo{host: host, offsetMap: make(map[string]map[structs.TopicPartition]int64)}
				}

				for cluster, tps := range offsetInfo.offsetMap {
					if _, present := this.lastHostOffsets[host].offsetMap[cluster]; !present {
						this.lastHostOffsets[host].offsetMap[cluster] = make(map[structs.TopicPartition]int64)
					}

					for tp, off := range tps {
						this.lastHostOffsets[host].offsetMap[cluster][tp] = off
					}
				}
			}
		} else {
			for host, offsetInfo := range offsets {
				for cluster, tps := range offsetInfo.offsetMap {
					for tp, off := range tps {
						this.lastHostOffsets[host].offsetMap[cluster][tp] = off - this.lastHostOffsets[host].offsetMap[cluster][tp]
					}
				}
			}
		}

	}

}

func (this *Balance) drawSummary() {
	for i := 0; i < 2; i++ {
		this.startAll()
		time.Sleep(this.interval)
		this.collectAll(i)
	}

	lines := []string{"Broker|Cluster|Topic|P|OPS"}
	for host, offsetInfo := range this.lastHostOffsets {
		for cluster, tps := range offsetInfo.offsetMap {
			for tp, off := range tps {
				if off < 5 {
					continue
				}

				lines = append(lines, fmt.Sprintf("%s|%s|%s|%d|%d", host, cluster, tp.Topic, tp.PartitionID, off))
			}
		}
	}
	this.Ui.Output(columnize.SimpleFormat(lines))
}

func (this *Balance) clusterTopProducers(zkcluster *zk.ZkCluster) {
	kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		return
	}
	defer kfk.Close()

	for {
		hostOffsets := make(map[string]hostOffsetInfo)

		topics, err := kfk.Topics()
		swallow(err)

		<-this.signalsCh[zkcluster.Name()]

		for _, topic := range topics {
			partions, err := kfk.WritablePartitions(topic)
			swallow(err)
			for _, partitionID := range partions {
				leader, err := kfk.Leader(topic, partitionID)
				swallow(err)

				latestOffset, err := kfk.GetOffset(topic, partitionID, sarama.OffsetNewest)
				swallow(err)

				host, _, err := net.SplitHostPort(leader.Addr())
				swallow(err)

				if _, present := hostOffsets[host]; !present {
					hostOffsets[host] = hostOffsetInfo{host: host, offsetMap: make(map[string]map[structs.TopicPartition]int64)}
				}
				if _, present := hostOffsets[host].offsetMap[zkcluster.Name()]; !present {
					hostOffsets[host].offsetMap[zkcluster.Name()] = make(map[structs.TopicPartition]int64)
				}

				tp := structs.TopicPartition{Topic: topic, PartitionID: partitionID}
				hostOffsets[host].offsetMap[zkcluster.Name()][tp] = latestOffset
			}
		}

		this.hostOffsetCh <- hostOffsets

		kfk.RefreshMetadata(topics...)
	}
}

func (*Balance) Synopsis() string {
	return "Balance topics distribution according to load instead of count"
}

func (this *Balance) Help() string {
	help := fmt.Sprintf(`
Usage: %s balance [options]

    %s

Options:

    -z zone
      Default %s

    -c cluster pattern

`, this.Cmd, this.Synopsis(), ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
