package command

import (
	"flag"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
)

type Topology struct {
	Ui          cli.Ui
	Cmd         string
	zone        string
	hostPattern string
	verbose     bool
}

func (this *Topology) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("topology", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.hostPattern, "host", "", "")
	cmdFlags.BoolVar(&this.verbose, "l", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.zone == "" {
		forSortedZones(func(zkzone *zk.ZkZone) {
			this.displayZoneTopology(zkzone)
		})

		return
	}

	// a single zone
	ensureZoneValid(this.zone)
	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	this.displayZoneTopology(zkzone)

	return
}

type brokerHostInfo struct {
	ports             []int
	leadingPartitions int                // being leader of how many partitions
	topics            map[string][]int32 // detailed leading topics info {topic: partitionIds}
	msgsInStock       int64
}

func newBrokerHostInfo() *brokerHostInfo {
	return &brokerHostInfo{
		ports:  make([]int, 0),
		topics: make(map[string][]int32),
	}
}

func (this *brokerHostInfo) addPort(port int) {
	this.ports = append(this.ports, port)
}

func (this *brokerHostInfo) addTopicPartition(topic string, partitionId int32) {
	if _, present := this.topics[topic]; !present {
		this.topics[topic] = []int32{partitionId}
	} else {
		this.topics[topic] = append(this.topics[topic], partitionId)
	}
}

func (this *Topology) displayZoneTopology(zkzone *zk.ZkZone) {
	this.Ui.Output(zkzone.Name())

	instances := make(map[string]*brokerHostInfo)
	zkzone.ForSortedBrokers(func(cluster string, brokers map[string]*zk.BrokerZnode) {
		if len(brokers) == 0 {
			return
		}

		for _, broker := range brokers {
			if !patternMatched(broker.Host, this.hostPattern) {
				continue
			}

			if _, present := instances[broker.Host]; !present {
				instances[broker.Host] = newBrokerHostInfo()
			}
			instances[broker.Host].addPort(broker.Port)
		}

		// find how many partitions a broker is leading
		zkcluster := zkzone.NewCluster(cluster)
		brokerList := zkcluster.BrokerList()
		if len(brokerList) == 0 {
			return
		}
		kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
		if err != nil {
			this.Ui.Error(color.Red("    %+v %s", brokerList, err.Error()))
			return
		}

		topics, err := kfk.Topics()
		swallow(err)
		for _, topic := range topics {
			partions, err := kfk.WritablePartitions(topic)
			swallow(err)
			for _, partitionID := range partions {
				leader, err := kfk.Leader(topic, partitionID)
				swallow(err)
				host, _, err := net.SplitHostPort(leader.Addr())
				swallow(err)
				if !patternMatched(host, this.hostPattern) {
					continue
				}

				latestOffset, err := kfk.GetOffset(topic, partitionID, sarama.OffsetNewest)
				swallow(err)
				oldestOffset, err := kfk.GetOffset(topic, partitionID, sarama.OffsetOldest)
				swallow(err)

				instances[host].msgsInStock += (latestOffset - oldestOffset)
				instances[host].leadingPartitions++
				instances[host].addTopicPartition(topic, partitionID)
			}
		}
	})

	// sort by host ip
	sortedHosts := make([]string, 0, len(instances))
	for host, _ := range instances {
		sortedHosts = append(sortedHosts, host)
	}
	sort.Strings(sortedHosts)

	for _, host := range sortedHosts {
		this.Ui.Output(fmt.Sprintf("  %s leading: %2dT %3dP %15sM ports %2d:%+v",
			color.Green("%15s", host),
			len(instances[host].topics),
			instances[host].leadingPartitions,
			gofmt.Comma(instances[host].msgsInStock),
			len(instances[host].ports),
			instances[host].ports))

		if this.verbose {
			for topic, partitions := range instances[host].topics {
				this.Ui.Output(fmt.Sprintf("%40s: P%+v", topic, partitions))
			}
		}
	}
}

func (*Topology) Synopsis() string {
	return "Print server topology and balancing stats of kafka clusters"
}

func (this *Topology) Help() string {
	help := fmt.Sprintf(`
Usage: %s topology [options]

    Print server topology and balancing stats of kafka clusters

Options:

    -z zone

    -host host pattern
      Display given hosts only.

    -l
      Use a long listing format.
`, this.Cmd)
	return strings.TrimSpace(help)
}
