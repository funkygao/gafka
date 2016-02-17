package command

import (
	"bufio"
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
	"github.com/funkygao/golib/pipestream"
)

type Topics struct {
	Ui  cli.Ui
	Cmd string

	topicPattern string
	verbose      bool
	topicN       int
	partitionN   int
	totalMsgs    int64
	totalOffsets int64
	ipInNumber   bool
}

func (this *Topics) Run(args []string) (exitCode int) {
	var (
		zone            string
		cluster         string
		addTopic        string
		replicas        int
		partitions      int
		configRetention int
		configged       bool
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.topicPattern, "t", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&this.verbose, "l", false, "")
	cmdFlags.BoolVar(&this.ipInNumber, "n", false, "")
	cmdFlags.StringVar(&addTopic, "add", "", "")
	cmdFlags.IntVar(&partitions, "partitions", 1, "")
	cmdFlags.BoolVar(&configged, "cf", false, "")
	cmdFlags.IntVar(&configRetention, "retention", -1, "")
	cmdFlags.IntVar(&replicas, "replicas", 2, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		on("-add", "-c").
		on("-retention", "-c", "-t").
		requireAdminRights("-add", "-retention").
		invalid(args) {
		return 2
	}

	if addTopic != "" {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		zkcluster := zkzone.NewCluster(cluster)
		swallow(this.addTopic(zkcluster, addTopic, replicas, partitions))

		return
	}

	ensureZoneValid(zone)

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	if configRetention > 0 {
		zkcluster := zkzone.NewCluster(cluster)
		this.configTopic(zkcluster, this.topicPattern, configRetention)
		return
	}

	if configged {
		// output header
		this.Ui.Output(fmt.Sprintf("%25s %40s %15s", "cluster", "topic", "mtime"))
		this.Ui.Output(fmt.Sprintf("%25s %40s %15s",
			strings.Repeat("-", 25), strings.Repeat("-", 40), strings.Repeat("-", 15)))

		displayTopicConfigs := func(zkcluster *zk.ZkCluster) {
			// sort by topic name
			configs := zkcluster.ConfiggedTopics()
			sortedTopics := make([]string, 0, len(configs))
			for topic, _ := range configs {
				sortedTopics = append(sortedTopics, topic)
			}
			sort.Strings(sortedTopics)

			for _, topic := range sortedTopics {
				configInfo := configs[topic]
				this.Ui.Output(fmt.Sprintf("%25s %40s %15s %s",
					zkcluster.Name(),
					topic,
					gofmt.PrettySince(configInfo.Mtime),
					configInfo.Config))
			}

		}

		if cluster == "" {
			zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
				displayTopicConfigs(zkcluster)
			})
		} else {
			zkcluster := zkzone.NewCluster(cluster)
			displayTopicConfigs(zkcluster)
		}

		return
	}

	if !this.verbose {
		// output header
		this.Ui.Output(fmt.Sprintf("%30s %50s", "cluster", "topic"))
		this.Ui.Output(fmt.Sprintf("%30s %50s",
			strings.Repeat("-", 30), strings.Repeat("-", 50)))
	}

	if cluster != "" {
		zkcluster := zkzone.NewCluster(cluster)
		this.displayTopicsOfCluster(zkcluster)

		this.Ui.Output(fmt.Sprintf("%25s %d", "-TOTAL Topics-", this.topicN))
		this.Ui.Output(fmt.Sprintf("%25s %d", "-TOTAL Partitions-", this.partitionN))
		if this.verbose {
			this.Ui.Output(fmt.Sprintf("%25s %s", "-FLAT Messages-", gofmt.Comma(this.totalMsgs)))
			this.Ui.Output(fmt.Sprintf("%25s %s", "-CUM Messages-", gofmt.Comma(this.totalOffsets)))
		}
		return
	}

	// all clusters
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		this.displayTopicsOfCluster(zkcluster)
	})
	this.Ui.Output(fmt.Sprintf("%25s %d", "-TOTAL Topics-", this.topicN))
	this.Ui.Output(fmt.Sprintf("%25s %d", "-TOTAL Partitions-", this.partitionN))
	if this.verbose {
		this.Ui.Output(fmt.Sprintf("%25s %s", "-FLAT Messages-", gofmt.Comma(this.totalMsgs)))
		this.Ui.Output(fmt.Sprintf("%25s %s", "-CUM Messages-", gofmt.Comma(this.totalOffsets)))
	}

	return
}

func (this *Topics) configTopic(zkcluster *zk.ZkCluster, topic string, retentionInMinute int) {
	/*
	  val SegmentBytesProp = "segment.bytes"
	  val SegmentMsProp = "segment.ms"
	  val SegmentIndexBytesProp = "segment.index.bytes"
	  val FlushMessagesProp = "flush.messages"
	  val FlushMsProp = "flush.ms"
	  val RetentionBytesProp = "retention.bytes"
	  val RententionMsProp = "retention.ms"
	  val MaxMessageBytesProp = "max.message.bytes"
	  val IndexIntervalBytesProp = "index.interval.bytes"
	  val DeleteRetentionMsProp = "delete.retention.ms"
	  val FileDeleteDelayMsProp = "file.delete.delay.ms"
	  val MinCleanableDirtyRatioProp = "min.cleanable.dirty.ratio"
	  val CleanupPolicyProp = "cleanup.policy"
	*/

	// ./bin/kafka-topics.sh --zookeeper localhost:2181/kafka --alter --topic foobar --config max.message.bytes=10000101
	// zk: {"version":1,"config":{"index.interval.bytes":"10000101","max.message.bytes":"10000101"}}

	if retentionInMinute < 10 {
		panic("less than 10 minutes?")
	}

	zkAddrs := zkcluster.ZkConnectAddr()
	key := "retention.ms"
	cmd := pipestream.New(fmt.Sprintf("%s/bin/kafka-topics.sh", ctx.KafkaHome()),
		fmt.Sprintf("--zookeeper %s", zkAddrs),
		fmt.Sprintf("--alter"),
		fmt.Sprintf("--topic %s", topic),
		fmt.Sprintf("--config %s=%d", key, retentionInMinute),
	)
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)

	output := make([]string, 0)
	for scanner.Scan() {
		output = append(output, scanner.Text())
	}
	swallow(scanner.Err())

	path := zkcluster.GetTopicConfigPath(topic)
	this.Ui.Info(path)

	for _, line := range output {
		this.Ui.Output(line)
	}

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
			color.Red("%s empty brokers", zkcluster.Name())), linesInTopicMode)
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
			if this.ipInNumber {
				linesInTopicMode = this.echoOrBuffer(fmt.Sprintf("%4s%s %s", " ",
					color.Green(brokerId), brokers[brokerId]), linesInTopicMode)
			} else {
				linesInTopicMode = this.echoOrBuffer(fmt.Sprintf("%4s%s %s", " ",
					color.Green(brokerId), brokers[brokerId].NamedString()), linesInTopicMode)
			}

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

	sortedTopics := make([]string, 0, len(topics))
	for _, t := range topics {
		sortedTopics = append(sortedTopics, t)
	}
	sort.Strings(sortedTopics)

	topicsCtime := zkcluster.TopicsCtime()
	hasTopicMatched := false
	for _, topic := range sortedTopics {
		if !patternMatched(topic, this.topicPattern) {
			continue
		}

		this.topicN++

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

		this.partitionN += len(partions)
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
				linesInTopicMode = this.echoOrBuffer(fmt.Sprintf("%8d Leader:%d Replicas:%+v Isr:%+v Offset:%16s - %-16s Num:%-15s",
					partitionID, leader.ID(), replicas, isr,
					gofmt.Comma(oldestOffset), gofmt.Comma(latestOffset), gofmt.Comma(latestOffset-oldestOffset)), linesInTopicMode)
				this.totalMsgs += latestOffset - oldestOffset
				this.totalOffsets += latestOffset
			} else {
				// use red for alert
				linesInTopicMode = this.echoOrBuffer(color.Red("%8d Leader:%d Replicas:%+v Isr:%+v Offset:%16s - %-16s Num:%-15s",
					partitionID, leader.ID(), replicas, isr,
					gofmt.Comma(oldestOffset), gofmt.Comma(latestOffset), gofmt.Comma(latestOffset-oldestOffset)), linesInTopicMode)
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
		this.Ui.Output(color.Yellow(l))
	}
	if this.ipInNumber {
		this.Ui.Output(fmt.Sprintf("\tzookeeper.connect: %s", zkcluster.ZkConnectAddr()))
		this.Ui.Output(fmt.Sprintf("\t      broker.list: %s",
			strings.Join(zkcluster.BrokerList(), ",")))
	} else {
		this.Ui.Output(fmt.Sprintf("\tzookeeper.connect: %s", zkcluster.NamedZkConnectAddr()))
		this.Ui.Output(fmt.Sprintf("\t      broker.list: %s",
			strings.Join(zkcluster.NamedBrokerList(), ",")))
	}

	return nil
}

func (*Topics) Synopsis() string {
	return "Manage kafka topics"
}

func (this *Topics) Help() string {
	help := fmt.Sprintf(`
Usage: %s topics [options]

    Manage kafka topics

Options:

    -z zone
      Default %s
  
    -c cluster

    -t topic name pattern
      Only show topics like this give topic.

    -cf
      Only show topics that have non-default configurations.    

    -add topic
      Add a topic to a kafka cluster.

    -partitions n
      Partition count when adding a new topic. Default 1.

    -replicas n
      Replica factor when adding a new topic. Default 2.

    -retention retention in minutes
      Config a topic log.retention.minutes.
    
    -l
      Use a long listing format.

    -n
      Show network addresses as numbers.
`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
