package command

import (
	"flag"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/pmylund/sortutil"
	"github.com/ryanuber/columnize"
)

type Clusters struct {
	Ui  cli.Ui
	Cmd string

	verbose           bool
	neat              bool
	registeredBrokers bool
	plainMode         bool
	publicOnly        bool
	ipInNumber        bool
}

func (this *Clusters) Run(args []string) (exitCode int) {
	var (
		addCluster     string
		setMode        bool
		verifyMode     bool
		public         int
		clusterName    string
		clusterPath    string
		zone           string
		priority       int
		retentionHours int
		port           string
		delCluster     string
		replicas       int
		addBroker      string
		nickname       string
		delBroker      string
		summaryMode    bool
	)
	cmdFlags := flag.NewFlagSet("clusters", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&addCluster, "add", "", "")
	cmdFlags.BoolVar(&this.publicOnly, "po", false, "")
	cmdFlags.BoolVar(&setMode, "s", false, "")
	cmdFlags.StringVar(&clusterName, "c", "", "")
	cmdFlags.StringVar(&clusterPath, "p", "", "")
	cmdFlags.BoolVar(&this.verbose, "l", false, "")
	cmdFlags.IntVar(&replicas, "replicas", -1, "")
	cmdFlags.StringVar(&delCluster, "del", "", "")
	cmdFlags.BoolVar(&summaryMode, "sum", false, "")
	cmdFlags.BoolVar(&this.neat, "neat", false, "")
	cmdFlags.IntVar(&retentionHours, "retention", -1, "")
	cmdFlags.BoolVar(&this.plainMode, "plain", false, "")
	cmdFlags.IntVar(&priority, "priority", -1, "")
	cmdFlags.IntVar(&public, "public", -1, "")
	cmdFlags.BoolVar(&this.ipInNumber, "n", false, "")
	cmdFlags.StringVar(&port, "port", "", "")
	cmdFlags.StringVar(&addBroker, "addbroker", "", "")
	cmdFlags.StringVar(&nickname, "nickname", "", "")
	cmdFlags.StringVar(&delBroker, "delbroker", "", "")
	cmdFlags.BoolVar(&this.registeredBrokers, "registered", false, "")
	cmdFlags.BoolVar(&verifyMode, "verify", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		on("-add", "-z", "-p").
		on("-s", "-z", "-c").
		requireAdminRights("-s", "-add").
		invalid(args) {
		return 2
	}

	if this.publicOnly {
		this.verbose = true
	}

	if delCluster != "" {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		zkcluster := zkzone.NewCluster(delCluster)

		kfkCluseterPath := zkzone.ClusterPath(delCluster)
		gafkaClusterInfoPath := zkcluster.ClusterInfoPath()
		gafkaClusterPath := zk.ClusterPath(delCluster)

		this.Ui.Info("shutdown brokers first: kill java/rm pkg/chkconfig --del")
		this.Ui.Info(fmt.Sprintf("zk rm -z %s -R -p %s",
			zone, kfkCluseterPath))
		this.Ui.Info(fmt.Sprintf("zk rm -z %s -p %s",
			zone, gafkaClusterPath))
		this.Ui.Info(fmt.Sprintf("zk rm -z %s -p %s",
			zone, gafkaClusterInfoPath))

		return
	}

	if addCluster != "" {
		// add cluster
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		defer printSwallowedErrors(this.Ui, zkzone)

		if err := zkzone.RegisterCluster(addCluster, clusterPath); err != nil {
			this.Ui.Error(err.Error())
			return 1
		}

		this.Ui.Info(fmt.Sprintf("%s: %s created", addCluster, clusterPath))

		return
	}

	if setMode {
		// setup a cluser meta info
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		zkcluster := zkzone.NewCluster(clusterName)
		if priority != -1 {
			zkcluster.SetPriority(priority)
		}
		if public != -1 {
			switch public {
			case 0:
				zkcluster.SetPublic(false)

			case 1:
				if nickname == "" {
					this.Ui.Error("-nickname required if set public a cluster, quit.")
					return
				}
				zkcluster.SetPublic(true)
			}
		}
		if retentionHours != -1 {
			zkcluster.SetRetention(retentionHours)
		}
		if replicas != -1 {
			zkcluster.SetReplicas(replicas)
		}
		if nickname != "" {
			zkcluster.SetNickname(nickname)
		}

		switch {
		case addBroker != "":
			parts := strings.Split(addBroker, ":")
			if len(parts) != 3 {
				this.Ui.Output(this.Help())
				return
			}

			brokerId, err := strconv.Atoi(parts[0])
			swallow(err)
			port, err := strconv.Atoi(parts[2])
			swallow(err)
			swallow(zkcluster.RegisterBroker(brokerId, parts[1], port))
			return

		case delBroker != "":
			for _, bid := range strings.Split(delBroker, ",") {
				brokerId, err := strconv.Atoi(strings.TrimSpace(bid))
				swallow(err)
				swallow(zkcluster.UnregisterBroker(brokerId))
			}
			return

		default:
			return
		}

	}

	if verifyMode {
		if zone != "" {
			zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
			this.verifyBrokers(zkzone)

			printSwallowedErrors(this.Ui, zkzone)
		} else {
			// print all zones all clusters
			forSortedZones(func(zkzone *zk.ZkZone) {
				this.verifyBrokers(zkzone)

				printSwallowedErrors(this.Ui, zkzone)
			})
		}

		return
	}

	if summaryMode {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		this.printSummary(zkzone, clusterName, port)
		return
	}

	// display mode
	if zone != "" {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		this.printClusters(zkzone, clusterName, port)

		printSwallowedErrors(this.Ui, zkzone)
	} else {
		// print all zones all clusters
		forSortedZones(func(zkzone *zk.ZkZone) {
			this.printClusters(zkzone, clusterName, port)

			printSwallowedErrors(this.Ui, zkzone)
		})
	}

	return
}

func (this *Clusters) printRegisteredBrokers(zkzone *zk.ZkZone) {
	this.Ui.Output(zkzone.Name())
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		info := zkcluster.RegisteredInfo()
		this.Ui.Output(fmt.Sprintf("    %s(%s)", info.Name(), info.Nickname))
		registeredBrokers := info.Roster
		if len(registeredBrokers) == 0 {
			this.Ui.Warn("        brokers not registered")
		} else {
			for _, b := range registeredBrokers {
				if this.ipInNumber {
					this.Ui.Output(fmt.Sprintf("        %2d %s", b.Id, b.Addr()))
				} else {
					this.Ui.Output(fmt.Sprintf("        %2d %s", b.Id, b.NamedAddr()))
				}
			}
		}

	})
}

func (this *Clusters) verifyBrokers(zkzone *zk.ZkZone) {
	this.Ui.Output(zkzone.Name())
	zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		zkcluster := zkzone.NewCluster(cluster)
		registeredBrokers := zkcluster.RegisteredInfo().Roster

		// find diff between registeredBrokers and liveBrokers
		// loop1 find liveBrokers>registeredBrokers
		for _, broker := range liveBrokers {
			foundInRoster := false
			for _, b := range registeredBrokers {
				bid := strconv.Itoa(b.Id)
				if bid == broker.Id && broker.Addr() == b.Addr() {
					foundInRoster = true
					break
				}
			}

			if !foundInRoster {
				// should manually register the broker
				this.Ui.Output(strings.Repeat(" ", 4) +
					color.Green("+ gk clusters -z %s -s -c %s -addbroker %s:%s",
						zkzone.Name(), cluster, broker.Id, broker.Addr()))
			}
		}

		// loop2 find liveBrokers<registeredBrokers
		for _, b := range registeredBrokers {
			foundInLive := false
			for _, broker := range liveBrokers {
				bid := strconv.Itoa(b.Id)
				if bid == broker.Id && broker.Addr() == b.Addr() {
					foundInLive = true
					break
				}
			}

			if !foundInLive {
				// the broker is dead
				this.Ui.Output(strings.Repeat(" ", 4) +
					color.Red("cluster[%s] broker[%d] %s is dead", cluster, b.Id, b.Addr()))
			}
		}
	})
}

func (this *Clusters) printSummary(zkzone *zk.ZkZone, clusterPattern string, port string) {
	lines := []string{"Zone|Cluster|Brokers|Topics|Partitions|FlatMsg|Cum"}

	type summary struct {
		zone, cluster               string
		brokers, topics, partitions int
		flat, cum                   int64
	}
	summaries := make([]summary, 0, 10)
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), clusterPattern) {
			return
		}

		brokers, topics, partitions, flat, cum := this.clusterSummary(zkcluster)
		summaries = append(summaries, summary{zkzone.Name(), zkcluster.Name(), brokers, topics, partitions,
			flat, cum})

	})
	sortutil.DescByField(summaries, "cum")
	var totalFlat, totalCum int64
	for _, s := range summaries {
		lines = append(lines, fmt.Sprintf("%s|%s|%d|%d|%d|%s|%s",
			s.zone, s.cluster, s.brokers, s.topics, s.partitions,
			gofmt.Comma(s.flat), gofmt.Comma(s.cum)))

		totalCum += s.cum
		totalFlat += s.flat
	}
	this.Ui.Output(columnize.SimpleFormat(lines))
	this.Ui.Output(fmt.Sprintf("Flat:%s Cum:%s", gofmt.Comma(totalFlat), gofmt.Comma(totalCum)))
}

func (this *Clusters) clusterSummary(zkcluster *zk.ZkCluster) (brokers, topics, partitions int, flat, cum int64) {
	brokerInfos := zkcluster.Brokers()
	brokers = len(brokerInfos)

	kfk, err := sarama.NewClient(zkcluster.BrokerList(), saramaConfig())
	if err != nil {
		this.Ui.Error(err.Error())
		return
	}
	defer kfk.Close()

	topicInfos, _ := kfk.Topics()
	topics = len(topicInfos)
	for _, t := range topicInfos {
		alivePartitions, _ := kfk.WritablePartitions(t)
		partitions += len(alivePartitions)

		for _, partitionID := range alivePartitions {
			latestOffset, _ := kfk.GetOffset(t, partitionID, sarama.OffsetNewest)
			oldestOffset, _ := kfk.GetOffset(t, partitionID, sarama.OffsetOldest)
			flat += (latestOffset - oldestOffset)
			cum += latestOffset
		}

	}

	return
}

func (this *Clusters) printClusters(zkzone *zk.ZkZone, clusterPattern string, port string) {
	if this.registeredBrokers {
		this.printRegisteredBrokers(zkzone)
		return
	}

	type clusterInfo struct {
		name, path         string
		nickname           string
		topicN, partitionN int
		err                string
		priority           int
		public             bool
		retention          int
		replicas           int
		brokerInfos        []zk.BrokerInfo
	}
	clusters := make([]clusterInfo, 0)
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), clusterPattern) {
			return
		}

		if this.plainMode {
			this.Ui.Output(zkcluster.Name())
			return
		}

		ci := clusterInfo{
			name: zkcluster.Name(),
			path: zkcluster.Chroot(),
		}
		if this.neat {
			clusters = append(clusters, ci)
			return
		}

		// verbose mode, will calculate topics and partition count
		brokerList := zkcluster.BrokerList()
		if len(brokerList) == 0 {
			ci.err = "no live brokers"
			clusters = append(clusters, ci)
			return
		}

		if port != "" {
			for _, hostport := range brokerList {
				_, p, err := net.SplitHostPort(hostport)
				swallow(err)

				if p != port {
					return
				}
			}
		}

		info := zkcluster.RegisteredInfo()
		if this.publicOnly && !info.Public {
			return
		}

		if !this.verbose {
			ci.brokerInfos = info.Roster
			clusters = append(clusters, ci)
			return
		}

		kfk, err := sarama.NewClient(brokerList, saramaConfig())
		if err != nil {
			ci.err = err.Error()
			clusters = append(clusters, ci)
			return
		}
		topics, err := kfk.Topics()
		if err != nil {
			ci.err = err.Error()
			clusters = append(clusters, ci)
			return
		}
		partitionN := 0
		for _, topic := range topics {
			partitions, err := kfk.Partitions(topic)
			if err != nil {
				ci.err = err.Error()
				clusters = append(clusters, ci)
				continue
			}

			partitionN += len(partitions)
		}

		clusters = append(clusters, clusterInfo{
			name:        zkcluster.Name(),
			nickname:    info.Nickname,
			path:        zkcluster.Chroot(),
			topicN:      len(topics),
			partitionN:  partitionN,
			retention:   info.Retention,
			public:      info.Public,
			replicas:    info.Replicas,
			priority:    info.Priority,
			brokerInfos: info.Roster,
		})
	})

	if this.plainMode {
		return
	}

	this.Ui.Output(fmt.Sprintf("%s: %d", zkzone.Name(), len(clusters)))
	if this.verbose {
		// 2 loop: 1. print the err clusters 2. print the good clusters
		for _, c := range clusters {
			if c.err == "" {
				continue
			}

			this.Ui.Output(fmt.Sprintf("%30s: %s %s", c.name, c.path,
				color.Red(c.err)))
		}

		// loop2
		for _, c := range clusters {
			if c.err != "" {
				continue
			}

			this.Ui.Output(fmt.Sprintf("%30s: %s",
				c.name, c.path))
			brokers := []string{}
			for _, broker := range c.brokerInfos {
				if this.ipInNumber {
					brokers = append(brokers, fmt.Sprintf("%d/%s:%d", broker.Id, broker.Host, broker.Port))
				} else {
					brokers = append(brokers, fmt.Sprintf("%d/%s", broker.Id, broker.NamedAddr()))
				}
			}
			if len(brokers) > 0 {
				sort.Strings(brokers)
				this.Ui.Info(color.Green("%31s %s", " ", strings.Join(brokers, ", ")))
			}

			this.Ui.Output(strings.Repeat(" ", 4) +
				color.Green("nick:%s public:%v topics:%d partitions:%d replicas:%d retention:%dh",
					c.nickname, c.public,
					c.topicN, c.partitionN, c.replicas, c.retention))
		}

		return
	}

	// not verbose mode
	hostsWithoutDnsRecords := make([]string, 0)
	for _, c := range clusters {
		this.Ui.Output(fmt.Sprintf("%30s: %s", c.name, c.path))
		brokers := []string{}
		for _, broker := range c.brokerInfos {
			if this.ipInNumber {
				brokers = append(brokers, fmt.Sprintf("%d/%s:%d", broker.Id, broker.Host, broker.Port))
			} else {
				brokers = append(brokers, fmt.Sprintf("%d/%s", broker.Id, broker.NamedAddr()))
			}

			if broker.Addr() == broker.NamedAddr() {
				hostsWithoutDnsRecords = append(hostsWithoutDnsRecords, fmt.Sprintf("%s:%s", c.name, broker.Addr()))
			}
		}
		if len(brokers) > 0 {
			sort.Strings(brokers)
			this.Ui.Info(color.Green("%31s %s", " ", strings.Join(brokers, ", ")))
		} else {
			this.Ui.Warn(fmt.Sprintf("%31s no live registered brokers", " "))
		}
	}

	if len(hostsWithoutDnsRecords) > 0 {
		this.Ui.Warn("brokers without dns record:")
		for _, broker := range hostsWithoutDnsRecords {
			parts := strings.SplitN(broker, ":", 2)
			this.Ui.Output(fmt.Sprintf("%30s: %s", parts[0], color.Yellow(parts[1])))
		}
	}

}

func (*Clusters) Synopsis() string {
	return "Register or display kafka clusters"
}

func (this *Clusters) Help() string {
	help := fmt.Sprintf(`
Usage: %s clusters [options]

    %s

Options:

    -z zone

    -c cluster name

    -sum
      Display summary of message.

    -l
      Use a long listing format.

    -port broker port number
      Search cluster with port number.

    -neat
      Use short listing format.

    -po
      Display only public clusters.

    -add cluster name
      Add a new kafka cluster into a zone.

    -del cluster name
      Help to delete a cluster.

    -n
      Show network addresses as numbers

    -p cluster zk path
      The new kafka cluster chroot path in Zookeeper.
      e,g. gk clusters -z prod -add foo -p /kafka/services/trade

    -s
      Enter cluster info setup mode.
    
    -priority n
      Set the priority of a cluster.

    -public <0|1>
      Export the cluster for PubSub system or not.
      e,g. gk clusters -z prod -c foo -s -public 1 -nickname foo

    -retention n hours
      log.retention.hours of kafka.

    -nickname name
      Set nickname of a cluster.
      e,g. gk clusters -z prod -c foo -s -nickname bar

    -replicas n
      Set the default replicas of a cluster. 
      Only works on meta data. To make kafka replica updated, use 'gk migrate'

    -addbroker id:host:port
      Register a permanent broker to a cluster.
      e,g. gk clusters -z prod -c foo -s -addbroker 0:10.1.2.3:10001

    -delbroker comma seperated broker ids
      Delete a broker from a cluster.
      e,g. gk clusters -z prod -c foo -s -delbroker 5,6

    -registered
      Display registered permanent brokers info.

    -verify
      Verify that the online brokers are consistent with the registered brokers.

    -plain
      Display cluster name only.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
