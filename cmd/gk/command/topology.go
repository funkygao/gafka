package command

import (
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Topology struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Topology) Run(args []string) (exitCode int) {
	if len(args) == 0 {
		this.Ui.Error("zone required")
		this.Ui.Output(this.Help())
		return 2
	}

	for _, zone := range args {
		ensureZoneValid(zone)

		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
		this.displayZoneTopology(zone, zkzone)
	}

	return
}

type brokerHostInfo struct {
	ports    []int
	leadingN int // being leader of how many partitions
}

func newBrokerHostInfo() *brokerHostInfo {
	return &brokerHostInfo{
		ports: make([]int, 0),
	}
}

func (this *brokerHostInfo) addPort(port int) {
	this.ports = append(this.ports, port)
}

func (this *Topology) swallow(err error) {
	if err != nil {
		panic(err)
	}
}

func (this *Topology) displayZoneTopology(zone string, zkzone *zk.ZkZone) {
	this.Ui.Output(zone)

	instances := make(map[string]*brokerHostInfo)
	zkzone.WithinBrokers(func(cluster string, brokers map[string]*zk.Broker) {
		if len(brokers) == 0 {
			return
		}

		for _, broker := range brokers {
			if _, present := instances[broker.Host]; !present {
				instances[broker.Host] = newBrokerHostInfo()
			}
			instances[broker.Host].addPort(broker.Port)
		}

		// find how many partitions a broker is leading
		zkcluster := zkzone.NewCluster(cluster)
		kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
		this.swallow(err)
		topics, err := kfk.Topics()
		this.swallow(err)
		for _, topic := range topics {
			partions, err := kfk.WritablePartitions(topic)
			this.swallow(err)
			for _, partitionID := range partions {
				leader, err := kfk.Leader(topic, partitionID)
				this.swallow(err)
				host, _, err := net.SplitHostPort(leader.Addr())
				this.swallow(err)
				instances[host].leadingN++
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
		this.Ui.Output(fmt.Sprintf("\t%s port:%+v leading:%sP",
			color.Green(host),
			instances[host].ports,
			color.Magenta("%d", instances[host].leadingN)))
	}
}

func (*Topology) Synopsis() string {
	return "Print server topology and balancing stats of kafka clusters"
}

func (this *Topology) Help() string {
	help := fmt.Sprintf(`
Usage: %s topology [zone ...]

	Print server topology and balancing stats of kafka clusters
`, this.Cmd)
	return strings.TrimSpace(help)
}
