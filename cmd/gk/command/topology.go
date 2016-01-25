package command

import (
	"flag"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
)

type Topology struct {
	Ui  cli.Ui
	Cmd string

	zone        string
	hostPattern string
	verbose     bool
	watchMode   bool
	maxPort     bool
}

func (this *Topology) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("topology", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.hostPattern, "host", "", "")
	cmdFlags.BoolVar(&this.verbose, "l", false, "")
	cmdFlags.BoolVar(&this.watchMode, "w", false, "")
	cmdFlags.BoolVar(&this.maxPort, "maxport", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.zone == "" {
		for {
			forSortedZones(func(zkzone *zk.ZkZone) {
				if this.maxPort {
					this.displayZoneMaxPort(zkzone)
				} else {
					this.displayZoneTopology(zkzone)
				}
			})

			if !this.watchMode {
				return
			}
			time.Sleep(time.Second * 5)
			refreshScreen()
		}

		return
	}

	// a single zone
	ensureZoneValid(this.zone)
	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	for {
		if this.maxPort {
			this.displayZoneMaxPort(zkzone)
		} else {
			this.displayZoneTopology(zkzone)
		}

		if !this.watchMode {
			return
		}
		time.Sleep(time.Second * 5)
		refreshScreen()
	}

	return
}

type brokerHostInfo struct {
	uptimes         map[int]time.Time  // key is broker tcp port
	tcpPorts        []int              // listening tcp ports
	topicPartitions map[string][]int32 // detailed leading topics info {topic: partitionIds}
	topicMsgs       map[string]int64
}

func newBrokerHostInfo() *brokerHostInfo {
	return &brokerHostInfo{
		tcpPorts:        make([]int, 0),
		uptimes:         make(map[int]time.Time),
		topicPartitions: make(map[string][]int32),
		topicMsgs:       make(map[string]int64),
	}
}

func (this *brokerHostInfo) leadingPartitions() int {
	r := 0
	for _, p := range this.topicPartitions {
		r += len(p)
	}
	return r
}

func (this *brokerHostInfo) totalMsgsInStock() int64 {
	r := int64(0)
	for _, n := range this.topicMsgs {
		r += n
	}
	return r
}

func (this *brokerHostInfo) addPort(port int, uptime time.Time) {
	this.tcpPorts = append(this.tcpPorts, port)
	this.uptimes[port] = uptime
}

func (this *brokerHostInfo) addTopicPartition(topic string, partitionId int32) {
	if _, present := this.topicPartitions[topic]; !present {
		this.topicPartitions[topic] = []int32{partitionId}
	} else {
		this.topicPartitions[topic] = append(this.topicPartitions[topic], partitionId)
	}
}

func (this *Topology) displayZoneMaxPort(zkzone *zk.ZkZone) {
	maxPort := 0
	zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		for _, broker := range liveBrokers {
			if maxPort < broker.Port {
				maxPort = broker.Port
			}
		}
	})

	this.Ui.Output(fmt.Sprintf("max port in zone[%s]: %d", zkzone.Name(), maxPort))
}

func (this *Topology) displayZoneTopology(zkzone *zk.ZkZone) {
	this.Ui.Output(zkzone.Name())

	// {cluster: {topic: brokerHostInfo}}
	brokerInstances := make(map[string]map[string]*brokerHostInfo)

	zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		if len(liveBrokers) == 0 {
			this.Ui.Warn(fmt.Sprintf("empty brokers in cluster[%s]", cluster))
			return
		}

		brokerInstances[cluster] = make(map[string]*brokerHostInfo)

		for _, broker := range liveBrokers {
			if !patternMatched(broker.Host, this.hostPattern) {
				continue
			}

			if _, present := brokerInstances[cluster][broker.Host]; !present {
				brokerInstances[cluster][broker.Host] = newBrokerHostInfo()
			}
			brokerInstances[cluster][broker.Host].addPort(broker.Port, broker.Uptime())
		}

		// find how many partitions a broker is leading
		zkcluster := zkzone.NewCluster(cluster)
		brokerList := zkcluster.BrokerList()
		if len(brokerList) == 0 {
			this.Ui.Warn(fmt.Sprintf("empty brokers in cluster[%s]", cluster))
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

				brokerInstances[cluster][host].topicMsgs[topic] += (latestOffset - oldestOffset)
				brokerInstances[cluster][host].addTopicPartition(topic, partitionID)
			}
		}
	})

	hosts := make(map[string]struct{})
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		for host, _ := range brokerInstances[zkcluster.Name()] {
			hosts[host] = struct{}{}
		}
	})
	sortedHosts := make([]string, 0)
	for host, _ := range hosts {
		sortedHosts = append(sortedHosts, host)
	}
	sort.Strings(sortedHosts)

	// sort by host ip
	sortedClusters := make([]string, 0, len(brokerInstances))
	for c, _ := range brokerInstances {
		sortedClusters = append(sortedClusters, c)
	}
	sort.Strings(sortedClusters)

	portN := 0
	hostN := 0
	topicN := 0
	partitionN := 0
	for _, host := range sortedHosts {
		tn := 0
		pn := 0
		mn := int64(0)
		ports := make([]int, 0)
		for _, cluster := range sortedClusters {
			if _, present := brokerInstances[cluster][host]; !present {
				continue
			}

			tn += len(brokerInstances[cluster][host].topicPartitions)
			pn += brokerInstances[cluster][host].leadingPartitions()
			mn += brokerInstances[cluster][host].totalMsgsInStock()
			ports = append(ports, brokerInstances[cluster][host].tcpPorts...)
		}

		portN += len(ports)
		topicN += tn
		partitionN += pn
		hostN += 1

		this.Ui.Output(fmt.Sprintf("  %s leading: %2dT %3dP %15sM ports %2d:%+v",
			color.Green("%15s", host),
			tn,
			pn,
			gofmt.Comma(mn),
			len(ports),
			ports))

		if this.verbose {
			for _, cluster := range sortedClusters {
				if _, present := brokerInstances[cluster][host]; !present {
					continue
				}

				for _, tcpPort := range brokerInstances[cluster][host].tcpPorts {
					this.Ui.Output(fmt.Sprintf("%40d %s", tcpPort,
						gofmt.PrettySince(brokerInstances[cluster][host].uptimes[tcpPort])))
				}
			}

			for _, cluster := range sortedClusters {
				if _, present := brokerInstances[cluster][host]; !present {
					continue
				}

				this.Ui.Output(color.Magenta("%30s", cluster))
				for topic, partitions := range brokerInstances[cluster][host].topicPartitions {
					this.Ui.Output(fmt.Sprintf("%40s: %15sM P%2d %+v",
						topic,
						gofmt.Comma(brokerInstances[cluster][host].topicMsgs[topic]),
						len(partitions), partitions))
				}

			}

		}
	}

	this.Ui.Output(fmt.Sprintf("%15s host:%d, topic:%d, partition:%d, instances:%d",
		"-TOTAL-",
		hostN, topicN, partitionN, portN))
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

    -w
      Run in watch mode: keep running till Ctrl^C.

    -l
      Use a long listing format.

    -maxport
      Display the max kafka broker tcp port in this zone.
`, this.Cmd)
	return strings.TrimSpace(help)
}
