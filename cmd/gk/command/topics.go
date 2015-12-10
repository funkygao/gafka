package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
)

type Topics struct {
	Ui           cli.Ui
	Cmd          string
	topicPattern string
	verbose      bool
}

func (this *Topics) Run(args []string) (exitCode int) {
	var (
		zone        string
		cluster     string
		addTopic    string
		replicas    int
		partitions  int
		topicConfig string
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&this.topicPattern, "t", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&this.verbose, "l", false, "")
	cmdFlags.StringVar(&addTopic, "add", "", "")
	cmdFlags.IntVar(&partitions, "partitions", 1, "")
	cmdFlags.StringVar(&topicConfig, "config", "", "")
	cmdFlags.IntVar(&replicas, "replicas", 2, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z").
		on("-add", "-c").
		requireAdminRights("-add", "-config").
		invalid(args) {
		return 2
	}

	if addTopic != "" {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		zkcluster := zkzone.NewCluster(cluster)
		this.addTopic(zkcluster, addTopic, replicas, partitions)

		return
	}

	ensureZoneValid(zone)

	if !this.verbose {
		// output header
		this.Ui.Output(fmt.Sprintf("%30s %50s", "cluster", "topic"))
		this.Ui.Output(fmt.Sprintf("%30s %50s",
			strings.Repeat("-", 30), strings.Repeat("-", 50)))
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	if cluster != "" {
		zkcluster := zkzone.NewCluster(cluster)
		this.displayTopicsOfCluster(zkcluster)
		return
	}

	// all clusters
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		this.displayTopicsOfCluster(zkcluster)
	})

	return
}

func (this *Topics) echoOrBuffer(line string, buffer []string) []string {
	if this.topicPattern != "" {
		// buffer
		return append(buffer, line)
	} else {
		this.Ui.Output(line)
		return nil
	}
}

func (this *Topics) displayTopicsOfCluster(zkcluster *zk.ZkCluster) {
	echoBuffer := func(lines []string) {
		for _, l := range lines {
			this.Ui.Output(l)
		}
	}

	linesInTopicMode := make([]string, 0)
	if this.verbose {
		linesInTopicMode = this.echoOrBuffer(zkcluster.Name(), linesInTopicMode)
	}

	// get all alive brokers within this cluster
	brokers := zkcluster.Brokers()
	if len(brokers) == 0 {
		linesInTopicMode = this.echoOrBuffer(fmt.Sprintf("%4s%s", " ",
			color.Red("empty brokers")), linesInTopicMode)
		echoBuffer(linesInTopicMode)
		return
	}

	if this.verbose {
		sortedBrokerIds := make([]string, 0, len(brokers))
		for brokerId, _ := range brokers {
			sortedBrokerIds = append(sortedBrokerIds, brokerId)
		}
		sort.Strings(sortedBrokerIds)
		for _, brokerId := range sortedBrokerIds {
			linesInTopicMode = this.echoOrBuffer(fmt.Sprintf("%4s%s %s", " ",
				color.Green(brokerId), brokers[brokerId]), linesInTopicMode)
		}
	}

	// find 1st broker in the cluster
	// each broker in the cluster has same metadata
	var broker0 *zk.BrokerZnode
	for _, broker := range brokers {
		broker0 = broker
		break
	}

	kfk, err := sarama.NewClient([]string{broker0.Addr()}, sarama.NewConfig())
	if err != nil {
		if this.verbose {
			linesInTopicMode = this.echoOrBuffer(color.Yellow("%5s%s %s", " ",
				broker0.Addr(), err.Error()), linesInTopicMode)
		}

		return
	}
	defer kfk.Close()

	topics, err := kfk.Topics()
	swallow(err)
	if len(topics) == 0 {
		if this.topicPattern == "" && this.verbose {
			linesInTopicMode = this.echoOrBuffer(fmt.Sprintf("%5s%s", " ",
				color.Magenta("no topics")), linesInTopicMode)
			echoBuffer(linesInTopicMode)
		}

		return
	}

	topicsCtime := zkcluster.TopicsCtime()
	hasTopicMatched := false
	for _, topic := range topics {
		if !patternMatched(topic, this.topicPattern) {
			continue
		}

		hasTopicMatched = true
		if this.verbose {
			linesInTopicMode = this.echoOrBuffer(strings.Repeat(" ", 4)+color.Blue(topic), linesInTopicMode)
		}

		// get partitions and check if some dead
		alivePartitions, err := kfk.WritablePartitions(topic)
		swallow(err)
		partions, err := kfk.Partitions(topic)
		swallow(err)
		if len(alivePartitions) != len(partions) {
			linesInTopicMode = this.echoOrBuffer(fmt.Sprintf("topic[%s] has %s partitions: %+v/%+v",
				alivePartitions, color.Red("dead"), partions), linesInTopicMode)
		}

		replicas, err := kfk.Replicas(topic, partions[0])
		swallow(err)

		if !this.verbose {
			linesInTopicMode = this.echoOrBuffer(fmt.Sprintf("%30s %s %3dP %dR %s",
				zkcluster.Name(),
				color.Blue("%50s", topic),
				len(partions), len(replicas),
				gofmt.PrettySince(topicsCtime[topic])), linesInTopicMode)
			continue
		}

		for _, partitionID := range alivePartitions {
			leader, err := kfk.Leader(topic, partitionID)
			swallow(err)

			replicas, err := kfk.Replicas(topic, partitionID)
			swallow(err)

			isr := zkcluster.Isr(topic, partitionID)
			//isr, err := kfk.Isr(topic, partitionID)

			underReplicated := false
			if len(isr) != len(replicas) {
				underReplicated = true
			}

			latestOffset, err := kfk.GetOffset(topic, partitionID,
				sarama.OffsetNewest)
			swallow(err)

			oldestOffset, err := kfk.GetOffset(topic, partitionID,
				sarama.OffsetOldest)
			swallow(err)

			if !underReplicated {
				linesInTopicMode = this.echoOrBuffer(fmt.Sprintf("%8d Leader:%d Replicas:%+v Isr:%+v Offset:%d Num:%d",
					partitionID, leader.ID(), replicas, isr,
					latestOffset, latestOffset-oldestOffset), linesInTopicMode)
			} else {
				// use red for alert
				linesInTopicMode = this.echoOrBuffer(color.Red("%8d Leader:%d Replicas:%+v Isr:%+v Offset:%d Num:%d",
					partitionID, leader.ID(), replicas, isr,
					latestOffset, latestOffset-oldestOffset), linesInTopicMode)
			}

		}
	}

	if this.topicPattern != "" {
		if hasTopicMatched {
			echoBuffer(linesInTopicMode)
		}

	} else {
		echoBuffer(linesInTopicMode)
	}
}

func (this *Topics) addTopic(zkcluster *zk.ZkCluster, topic string, replicas,
	partitions int) error {
	this.Ui.Info(fmt.Sprintf("creating kafka topic: %s", topic))

	lines, err := zkcluster.AddTopic(topic, replicas, partitions)
	if err != nil {
		return err
	}

	for _, l := range lines {
		this.Ui.Info(l)
	}
	this.Ui.Output(fmt.Sprintf("\tzookeeper.connect: %s", zkcluster.ZkConnectAddr()))
	this.Ui.Output(fmt.Sprintf("\t      broker list: %s",
		strings.Join(zkcluster.BrokerList(), ",")))
	return nil
}

func (*Topics) Synopsis() string {
	return "Manage topics & partitions of a zone"
}

func (this *Topics) Help() string {
	help := fmt.Sprintf(`
Usage: %s topics -z zone [options]

    Manage topics & partitions of a zone

Options:
  
    -c cluster

    -t topic name pattern
      Only show topics like this give topic.

    -config k=v
      Config a topic. TODO

    -add topic
      Add a topic to a kafka cluster.

    -partitions n
      Partition count when adding a new topic. Default 1.

    -replicas n
      Replica factor when adding a new topic. Default 2.

    -l
      Use a long listing format.
`, this.Cmd)
	return strings.TrimSpace(help)
}
