package command

import (
	"bufio"
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/pipestream"
	"github.com/funkygao/sarama"
)

type Topics struct {
	Ui          cli.Ui
	Cmd         string
	topicPrefix string
	verbose     bool
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
	cmdFlags.StringVar(&this.topicPrefix, "t", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&this.verbose, "verbose", false, "")
	cmdFlags.StringVar(&addTopic, "add", "", "")
	cmdFlags.IntVar(&partitions, "partitions", 1, "")
	cmdFlags.StringVar(&topicConfig, "config", "", "")
	cmdFlags.IntVar(&replicas, "replicas", 2, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).require("-z").on("-add", "-c").invalid(args) {
		return 2
	}

	if addTopic != "" {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
		zkcluster := zkzone.NewCluster(cluster)
		this.addTopic(zkcluster, addTopic, replicas, partitions)

		return
	}

	ensureZoneValid(zone)

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
	if cluster != "" {
		zkcluster := zkzone.NewCluster(cluster)
		this.displayTopicsOfCluster(zkcluster)
		return
	}

	// all clusters
	zkzone.WithinClusters(func(cluster string, path string) {
		zkcluster := zkzone.NewclusterWithPath(cluster, path)
		this.displayTopicsOfCluster(zkcluster)
	})

	return
}

func (this *Topics) displayTopicsOfCluster(zkcluster *zk.ZkCluster) {
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	if this.verbose {
		this.Ui.Output(zkcluster.Name())
	}

	// get all alive brokers within this cluster
	brokers := zkcluster.Brokers()
	if len(brokers) == 0 {
		this.Ui.Output(fmt.Sprintf("%4s%s", " ", color.Red("empty brokers")))
		return
	}

	if this.verbose {
		sortedBrokerIds := make([]string, 0, len(brokers))
		for brokerId, _ := range brokers {
			sortedBrokerIds = append(sortedBrokerIds, brokerId)
		}
		sort.Strings(sortedBrokerIds)
		for _, brokerId := range sortedBrokerIds {
			this.Ui.Output(fmt.Sprintf("%4s%s %s", " ", color.Green(brokerId),
				brokers[brokerId]))
		}
	}

	// find 1st broker in the cluster
	// each broker in the cluster has same metadata
	var broker0 *zk.Broker
	for _, broker := range brokers {
		broker0 = broker
		break
	}

	kfk, err := sarama.NewClient([]string{broker0.Addr()}, sarama.NewConfig())
	if err != nil {
		if this.verbose {
			this.Ui.Output(color.Yellow("%5s%s %s", " ", broker0.Addr(),
				err.Error()))
		}

		return
	}
	defer kfk.Close()

	topics, err := kfk.Topics()
	must(err)
	if len(topics) == 0 {
		if this.topicPrefix == "" && this.verbose {
			this.Ui.Output(fmt.Sprintf("%5s%s", " ", color.Magenta("no topics")))
		}

		return
	}

	for _, topic := range topics {
		if this.topicPrefix != "" && !strings.HasPrefix(topic, this.topicPrefix) {
			continue
		}

		if this.verbose {
			this.Ui.Output(strings.Repeat(" ", 4) + color.Blue(topic))
		}

		// get partitions and check if some dead
		alivePartitions, err := kfk.WritablePartitions(topic)
		must(err)
		partions, err := kfk.Partitions(topic)
		must(err)
		if len(alivePartitions) != len(partions) {
			this.Ui.Output(fmt.Sprintf("topic[%s] has %s partitions: %+v/%+v",
				alivePartitions, color.Red("dead"), partions))
		}

		replicas, err := kfk.Replicas(topic, partions[0])
		must(err)

		if !this.verbose {
			this.Ui.Output(fmt.Sprintf("%30s %s %3dP %dR",
				zkcluster.Name(),
				color.Blue("%50s", topic),
				len(partions), len(replicas)))
			continue
		}

		for _, partitionID := range alivePartitions {
			leader, err := kfk.Leader(topic, partitionID)
			must(err)

			replicas, err := kfk.Replicas(topic, partitionID)
			must(err)

			isr := zkcluster.Isr(topic, partitionID)
			//isr, err := kfk.Isr(topic, partitionID)

			underReplicated := false
			if len(isr) != len(replicas) {
				underReplicated = true
			}

			latestOffset, err := kfk.GetOffset(topic, partitionID,
				sarama.OffsetNewest)
			must(err)

			oldestOffset, err := kfk.GetOffset(topic, partitionID,
				sarama.OffsetOldest)
			must(err)

			if !underReplicated {
				this.Ui.Output(fmt.Sprintf("%8d Leader:%d Replicas:%+v Isr:%+v Offset:%d Num:%d",
					partitionID, leader.ID(), replicas, isr,
					latestOffset, latestOffset-oldestOffset))
			} else {
				// use red for alert
				this.Ui.Output(color.Red("%8d Leader:%d Replicas:%+v Isr:%+v Offset:%d Num:%d",
					partitionID, leader.ID(), replicas, isr,
					latestOffset, latestOffset-oldestOffset))
			}

		}
	}
}

func (this *Topics) addTopic(zkcluster *zk.ZkCluster, topic string, replicas,
	partitions int) error {
	this.Ui.Info(fmt.Sprintf("creating kafka topic: %s", topic))

	zkAddrs := zkcluster.ZkAddrs()

	cmd := pipestream.New(fmt.Sprintf("%s/bin/kafka-topics.sh", ctx.KafkaHome()),
		fmt.Sprintf("--zookeeper %s", zkAddrs),
		fmt.Sprintf("--create"),
		fmt.Sprintf("--topic %s", topic),
		fmt.Sprintf("--partitions %d", partitions),
		fmt.Sprintf("--replication-factor %d", replicas),
	)
	err := cmd.Open()
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	var errmsg string
	var line string
	for scanner.Scan() {
		line = scanner.Text()

		this.Ui.Info(line)
		if strings.HasPrefix(line, "Error") {
			errmsg = line
		}
	}
	err = scanner.Err()
	if err != nil {
		return err
	}
	cmd.Close()

	if errmsg != "" {
		return fmt.Errorf("%s", errmsg)
	}

	this.Ui.Output(fmt.Sprintf("\tzookeeper.connect: %s", zkAddrs))
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

  -t topic prefix
  	Only show topics like this give topic.

  -config k=v
  	Config a topic. TODO

  -add topic
  	Add a topic to a kafka cluster.

  -partitions n
  	Default 1

  -replicas n
  	Default 2

  -verbose
`, this.Cmd)
	return strings.TrimSpace(help)
}
