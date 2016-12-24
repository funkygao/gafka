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
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/pmylund/sortutil"
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

func (ho hostOffsetInfo) Clusters() []string {
	var r []string
	for cluster, _ := range ho.offsetMap {
		r = append(r, cluster)
	}
	return r
}

func (ho hostOffsetInfo) Total() (t int64) {
	for _, tps := range ho.offsetMap {
		for _, qps := range tps {
			t += qps
		}
	}

	return
}

func (ho hostOffsetInfo) ClusterPartitions(cluster string) int {
	return len(ho.offsetMap[cluster])
}

func (ho hostOffsetInfo) ClusterTotal(cluster string) (t int64) {
	for _, qps := range ho.offsetMap[cluster] {
		t += qps
	}

	return
}

type Balance struct {
	Ui  cli.Ui
	Cmd string

	zone, cluster string
	interval      time.Duration
	detailMode    bool
	host          string
	atLeastTps    int64

	offsets     map[string]int64 // host => offset sum TODO
	lastOffsets map[string]int64

	allHostsTps map[string]hostOffsetInfo

	hostOffsetCh chan map[string]hostOffsetInfo // key is host
	signalsCh    map[string]chan struct{}
}

func (this *Balance) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("balance", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.DurationVar(&this.interval, "i", time.Second*5, "")
	cmdFlags.StringVar(&this.host, "host", "", "")
	cmdFlags.Int64Var(&this.atLeastTps, "over", 0, "")
	cmdFlags.BoolVar(&this.detailMode, "d", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.signalsCh = make(map[string]chan struct{})
	this.hostOffsetCh = make(chan map[string]hostOffsetInfo)

	this.allHostsTps = make(map[string]hostOffsetInfo)
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

	this.drawBalance()

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
			// record into allHostsTps
			for host, offsetInfo := range offsets {
				if _, present := this.allHostsTps[host]; !present {
					this.allHostsTps[host] = hostOffsetInfo{host: host, offsetMap: make(map[string]map[structs.TopicPartition]int64)}
				}

				for cluster, tps := range offsetInfo.offsetMap {
					if _, present := this.allHostsTps[host].offsetMap[cluster]; !present {
						this.allHostsTps[host].offsetMap[cluster] = make(map[structs.TopicPartition]int64)
					}

					for tp, off := range tps {
						// 1st loop, offset
						this.allHostsTps[host].offsetMap[cluster][tp] = off
					}
				}
			}
		} else {
			for host, offsetInfo := range offsets {
				for cluster, tps := range offsetInfo.offsetMap {
					for tp, off := range tps {
						// 2nd loop, qps
						// FIXME hard coding
						this.allHostsTps[host].offsetMap[cluster][tp] = (off - this.allHostsTps[host].offsetMap[cluster][tp]) / int64(this.interval.Seconds())
					}
				}
			}
		}
	}

}

func (this *Balance) drawBalance() {
	for i := 0; i < 2; i++ {
		this.startAll()
		time.Sleep(this.interval)
		this.collectAll(i)
	}

	type hostTps struct {
		host string
		tps  int64
	}
	var sortedHosts []hostTps
	for host, info := range this.allHostsTps {
		sortedHosts = append(sortedHosts, hostTps{host, info.Total()})
	}
	sortutil.AscByField(sortedHosts, "tps")

	var hosts []string
	for _, h := range sortedHosts {
		if this.atLeastTps > 0 && h.tps < this.atLeastTps {
			continue
		}

		hosts = append(hosts, h.host)
	}

	if !this.detailMode {
		this.drawSummary(hosts)
		return
	}

	this.drawDetail(hosts)
}

func (this *Balance) drawDetail(sortedHosts []string) {
	type hostSummary struct {
		cluster string
		tp      structs.TopicPartition
		qps     int64
	}

	for _, host := range sortedHosts {
		var summary []hostSummary
		offsetInfo := this.allHostsTps[host]

		for cluster, tps := range offsetInfo.offsetMap {
			for tp, qps := range tps {
				summary = append(summary, hostSummary{cluster, tp, qps})
			}
		}

		sortutil.DescByField(summary, "qps")

		this.Ui.Output(color.Green("%16s %8s %+v", host, gofmt.Comma(offsetInfo.Total()), offsetInfo.Clusters()))
		for _, sum := range summary {
			if sum.qps < 100 {
				continue
			}
			this.Ui.Output(fmt.Sprintf("    %30s %8s %s", sum.cluster, gofmt.Comma(sum.qps), sum.tp))
		}
	}

}

type clusterQps struct {
	cluster    string
	qps        int64
	partitions int
}

func (c clusterQps) String() string {
	partitions := fmt.Sprintf("%d", c.partitions)
	if c.qps < 1000 {
		return fmt.Sprintf("%s#%s/%d", c.cluster, partitions, c.qps)
	} else if c.qps < 5000 {
		return fmt.Sprintf("%s#%s/%s", c.cluster, partitions, color.Magenta("%d", c.qps))
	} else if c.qps < 10000 {
		return fmt.Sprintf("%s#%s/%s", c.cluster, partitions, color.Yellow("%d", c.qps))
	}

	return fmt.Sprintf("%s/%s/%s", c.cluster, partitions, color.Red("%d", c.qps))
}

func (this *Balance) drawSummary(sortedHosts []string) {
	lines := []string{"Broker|P|TPS|Cluster/OPS"}
	var totalTps int64
	var totalPartitions int
	for _, host := range sortedHosts {
		hostPartitions := 0
		offsetInfo := this.allHostsTps[host]
		var clusters []clusterQps
		for cluster, _ := range offsetInfo.offsetMap {
			clusterTps := offsetInfo.ClusterTotal(cluster)
			clusterPartitions := offsetInfo.ClusterPartitions(cluster)
			hostPartitions += clusterPartitions
			totalTps += clusterTps
			totalPartitions += clusterPartitions

			clusters = append(clusters, clusterQps{cluster, clusterTps, clusterPartitions})
		}

		sortutil.AscByField(clusters, "cluster")

		lines = append(lines, fmt.Sprintf("%s|%d|%s|%+v",
			host, hostPartitions, gofmt.Comma(offsetInfo.Total()), clusters))
	}
	this.Ui.Output(columnize.SimpleFormat(lines))
	this.Ui.Output(fmt.Sprintf("-Total- Hosts:%d Partitions:%d Tps:%s",
		len(sortedHosts), totalPartitions, gofmt.Comma(totalTps)))
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

				if !patternMatched(host, this.host) {
					continue
				}

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

    -host broker ip

    -d
      Display in detailed mode.

    -over number
      Only display brokers whose TPS over the number.

`, this.Cmd, this.Synopsis(), ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
