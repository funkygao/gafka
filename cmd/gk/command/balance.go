package command

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/columnize"
	"github.com/funkygao/gafka/cmd/kateway/structs"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/golib/pipestream"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/pmylund/sortutil"
)

type hostLoadInfo struct {
	host            string
	cluster         string
	topicPartitions []structs.TopicPartition
	qps             int64
}

type hostOffsetInfo struct {
	host      string
	brokerIDs map[string]int32                            // cluster:brokerID
	offsetMap map[string]map[structs.TopicPartition]int64 // cluster:tp:offset
}

func (ho hostOffsetInfo) Clusters() []string {
	var r []string
	for cluster, _ := range ho.offsetMap {
		r = append(r, cluster)
	}
	return r
}

func (ho hostOffsetInfo) MightProblematic() bool {
	if len(ho.offsetMap) < 2 {
		return false
	}

	bigClusters := 0
	for cluster, _ := range ho.offsetMap {
		if ho.ClusterTotal(cluster) > 1000 {
			bigClusters++
		}
	}
	return bigClusters > 1
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

type brokerModel struct {
	disks    int
	nicSpeed int
}

type Balance struct {
	Ui  cli.Ui
	Cmd string

	zone, cluster     string
	interval          time.Duration
	detailMode        bool
	host              string
	atLeastTps        int64
	hideZeroClusetr   bool
	skipKafkaInternal bool
	byCluster         bool

	loadAvgMap   map[string]float64
	loadAvgReady chan struct{}

	brokerModelMap   map[string]*brokerModel
	brokerModelReady chan struct{}

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
	cmdFlags.BoolVar(&this.hideZeroClusetr, "nozero", false, "")
	cmdFlags.BoolVar(&this.byCluster, "bycluster", false, "")
	cmdFlags.DurationVar(&this.interval, "i", time.Second*5, "")
	cmdFlags.StringVar(&this.host, "host", "", "")
	cmdFlags.BoolVar(&this.skipKafkaInternal, "skipk", true, "")
	cmdFlags.Int64Var(&this.atLeastTps, "over", 0, "")
	cmdFlags.BoolVar(&this.detailMode, "l", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.brokerModelMap = make(map[string]*brokerModel)
	this.brokerModelReady = make(chan struct{})

	this.loadAvgReady = make(chan struct{})
	this.loadAvgMap = make(map[string]float64)

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
					this.allHostsTps[host] = hostOffsetInfo{
						host:      host,
						brokerIDs: make(map[string]int32),
						offsetMap: make(map[string]map[structs.TopicPartition]int64),
					}
				}

				for cluster, tps := range offsetInfo.offsetMap {
					if _, present := this.allHostsTps[host].offsetMap[cluster]; !present {
						this.allHostsTps[host].offsetMap[cluster] = make(map[structs.TopicPartition]int64)
					}

					for tp, off := range tps {
						// 1st loop, offset
						this.allHostsTps[host].offsetMap[cluster][tp] = off
						this.allHostsTps[host].brokerIDs[cluster] = offsetInfo.brokerIDs[cluster]
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
	go this.fetchLoadAvg()
	go this.fetchBrokerModel()

	for i := 0; i < 2; i++ {
		this.startAll()
		time.Sleep(this.interval)
		this.collectAll(i)
	}

	if this.byCluster {
		this.drawClusterTps()
		return
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

	<-this.loadAvgReady
	<-this.brokerModelReady

	if !this.detailMode {
		this.drawSummary(hosts)
		return
	}

	this.drawDetail(hosts)
}

func (this *Balance) drawClusterTps() {
	clusters := make(map[string]int64)
	type clusterInfo struct {
		cluster string
		qps     int64
	}

	for _, info := range this.allHostsTps {
		for cluster, _ := range info.offsetMap {
			clusters[cluster] += info.ClusterTotal(cluster)
		}
	}

	var cis []clusterInfo
	for cluster, qps := range clusters {
		cis = append(cis, clusterInfo{cluster, qps})
	}

	sortutil.AscByField(cis, "qps")

	lines := []string{"Cluster|TPS"}
	for _, c := range cis {
		lines = append(lines, fmt.Sprintf("%s|%d", c.cluster, c.qps))
	}
	this.Ui.Output(columnize.SimpleFormat(lines))
}

func (this *Balance) drawDetail(sortedHosts []string) {
	type hostSummary struct {
		cluster  string
		tp       structs.TopicPartition
		qps      int64
		brokerID int32
	}

	for _, host := range sortedHosts {
		var summary []hostSummary
		offsetInfo := this.allHostsTps[host]

		for cluster, tps := range offsetInfo.offsetMap {
			for tp, qps := range tps {
				summary = append(summary, hostSummary{cluster, tp, qps, offsetInfo.brokerIDs[cluster]})
			}
		}

		sortutil.DescByField(summary, "qps")

		this.Ui.Output(color.Green("%16s %8s %+v", host, gofmt.Comma(offsetInfo.Total()), offsetInfo.Clusters()))
		for _, sum := range summary {
			if sum.qps < 100 {
				continue
			}

			this.Ui.Output(fmt.Sprintf("    %30s %2d %8s %s#%d", sum.cluster, sum.brokerID,
				gofmt.Comma(sum.qps), sum.tp.Topic, sum.tp.PartitionID))
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
		return fmt.Sprintf("%s#%s/%s", c.cluster, partitions, color.Yellow("%d", c.qps))
	} else if c.qps < 10000 {
		return fmt.Sprintf("%s#%s/%s", c.cluster, partitions, color.Magenta("%d", c.qps))
	}

	return fmt.Sprintf("%s/%s/%s", c.cluster, partitions, color.Red("%d", c.qps))
}

func (this *Balance) drawSummary(sortedHosts []string) {
	lines := []string{"Broker|Load|D|P|P/D|Net|TPS|Cluster/OPS"}
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

			if this.hideZeroClusetr && clusterTps == 0 {
				continue
			}

			clusters = append(clusters, clusterQps{cluster, clusterTps, clusterPartitions})
		}

		sortutil.AscByField(clusters, "cluster")

		load := fmt.Sprintf("%-4.1f", this.loadAvgMap[host])
		if this.loadAvgMap[host] > 2. {
			load = color.Red("%-4.1f", this.loadAvgMap[host])
		}

		model := this.brokerModelMap[host]
		disks := fmt.Sprintf("%-2d", model.disks)
		if model.disks < 3 {
			// kafka need more disks
			disks = color.Cyan("%-2d", model.disks)
		}

		if offsetInfo.MightProblematic() {
			host = color.Yellow("%-15s", host) // hack for color output alignment
		}

		partitionsPerDisk := 0
		if model.disks > 0 {
			partitionsPerDisk = hostPartitions / model.disks
		}
		ppd := fmt.Sprintf("%-3d", partitionsPerDisk)
		if offsetInfo.Total() > 5000 && partitionsPerDisk > 5 {
			ppd = color.Red("%-3d", partitionsPerDisk)
		}

		lines = append(lines, fmt.Sprintf("%s|%s|%s|%d|%s|%d|%s|%+v",
			host, load, disks, hostPartitions, ppd, model.nicSpeed/1000,
			gofmt.Comma(offsetInfo.Total()), clusters))
	}

	this.Ui.Output(columnize.SimpleFormat(lines))
	this.Ui.Output(fmt.Sprintf("-Total- Hosts:%d Partitions:%d Tps:%s",
		len(sortedHosts), totalPartitions, gofmt.Comma(totalTps)))
}

func (this *Balance) fetchBrokerModel() {
	defer close(this.brokerModelReady)

	// get members host ip
	cf := consulapi.DefaultConfig()
	client, _ := consulapi.NewClient(cf)
	members, _ := client.Agent().Members(false)

	nodeHostMap := make(map[string]string, len(members))
	for _, member := range members {
		nodeHostMap[member.Name] = member.Addr
	}

	// get NIC speed
	cmd := pipestream.New("consul", "exec", "ethtool", "bond0", "|", "grep", "Speed")
	cmd.Open()
	if cmd.Reader() == nil {
		return
	}

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "finished with exit code 0") ||
			strings.Contains(line, "completed / acknowledged") {
			continue
		}
		fields := strings.Fields(line)
		node := fields[0]
		parts := strings.Split(line, "Speed:")
		if len(parts) < 2 {
			continue
		}
		if strings.HasSuffix(node, ":") {
			node = strings.TrimRight(node, ":")
		}
		host := nodeHostMap[node]

		speedStr := strings.TrimSpace(parts[1]) // 20000Mb/s
		tuples := strings.Split(speedStr, "Mb/s")
		speed, _ := strconv.Atoi(tuples[0])

		this.brokerModelMap[host] = &brokerModel{nicSpeed: speed}
	}
	cmd.Close()

	// get disks count
	cmd = pipestream.New("consul", "exec", "df")
	cmd.Open()
	if cmd.Reader() == nil {
		return
	}

	scanner = bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "finished with exit code 0") ||
			strings.Contains(line, "completed / acknowledged") ||
			strings.Contains(line, "/home") ||
			strings.Contains(line, "/boot") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		node := fields[0]
		if strings.HasSuffix(node, ":") {
			node = strings.TrimRight(node, ":")
		}
		host := nodeHostMap[node]
		if host == "" {
			continue
		}

		if strings.HasPrefix(strings.TrimSpace(fields[1]), "/dev/") {
			// kafka storage fs
			broker := this.brokerModelMap[host]
			if broker == nil {
				// this host might not be broker: it has no bond0
				continue
			}

			broker.disks++
		}
	}
	cmd.Close()
}

func (this *Balance) fetchLoadAvg() {
	defer close(this.loadAvgReady)

	// get members host ip
	cf := consulapi.DefaultConfig()
	client, _ := consulapi.NewClient(cf)
	members, _ := client.Agent().Members(false)

	nodeHostMap := make(map[string]string, len(members))
	for _, member := range members {
		nodeHostMap[member.Name] = member.Addr
	}

	cmd := pipestream.New("consul", "exec", "uptime", "|", "grep", "load")
	cmd.Open()
	if cmd.Reader() == nil {
		return
	}
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		node := fields[0]
		parts := strings.Split(line, "load average:")
		if len(parts) < 2 {
			continue
		}
		if strings.HasSuffix(node, ":") {
			node = strings.TrimRight(node, ":")
		}

		load1m, err := ctx.ExtractLoadAvg1m(line)
		if err != nil {
			continue
		}

		host := nodeHostMap[node]
		this.loadAvgMap[host] = load1m
	}

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
			if topic == "__consumer_offsets" {
				// skip kafka intennal topics
				continue
			}

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
					hostOffsets[host] = hostOffsetInfo{
						host:      host,
						offsetMap: make(map[string]map[structs.TopicPartition]int64),
						brokerIDs: make(map[string]int32),
					}
				}
				if _, present := hostOffsets[host].offsetMap[zkcluster.Name()]; !present {
					hostOffsets[host].offsetMap[zkcluster.Name()] = make(map[structs.TopicPartition]int64)
					hostOffsets[host].brokerIDs[zkcluster.Name()] = leader.ID()
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

    -l
      Use a long listing format.

    -over number
      Only display brokers whose TPS over the number.

    -nozero
      Hide 0 OPS clusters. False by default.

    -bycluster      

    -skipk
      Skip kafka internal topic: __consumer_offsets. True by default.

`, this.Cmd, this.Synopsis(), ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
