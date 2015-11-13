package command

import (
	"bufio"
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/pipestream"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/sarama"
)

type Topics struct {
	Ui cli.Ui
}

func (this *Topics) Run(args []string) (exitCode int) {
	var (
		zone         string
		cluster      string
		topicPattern string
		verbose      bool
		addTopic     string
		replicas     int
		partitions   int
		topicConfig  string
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&topicPattern, "t", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&verbose, "verbose", false, "")
	cmdFlags.StringVar(&addTopic, "add", "", "")
	cmdFlags.IntVar(&partitions, "partitions", 1, "")
	cmdFlags.StringVar(&topicConfig, "config", "", "")
	cmdFlags.IntVar(&replicas, "replicas", 2, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" {
		this.Ui.Error("empty zone not allowed")
		this.Ui.Output(this.Help())
		return 2
	}

	if addTopic != "" {
		if cluster == "" {
			this.Ui.Error("to add a topic, -c cluster required")
			this.Ui.Output(this.Help())
			return 2
		}

		zkAddrs := config.ZonePath(zone)
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
		zkAddrs = zkAddrs + zkzone.ClusterPath(cluster)
		this.addTopic(zkAddrs, addTopic, replicas, partitions)

		return
	}

	ensureZoneValid(zone)

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
	if cluster != "" {
		this.displayTopicsOfCluster(cluster, zkzone, topicPattern, verbose)
		return
	}

	// all clusters
	zkzone.WithinClusters(func(cluster string, path string) {
		this.displayTopicsOfCluster(cluster, zkzone, topicPattern, verbose)
	})

	return
}

func (this *Topics) displayTopicsOfCluster(cluster string, zkzone *zk.ZkZone,
	topicPattern string, verbose bool) {
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	this.Ui.Output(cluster)
	zkcluster := zkzone.NewCluster(cluster)

	// get all alive brokers within this cluster
	brokers := zkcluster.Brokers()
	if len(brokers) == 0 {
		this.Ui.Output(fmt.Sprintf("%4s%s", " ", color.Red("empty brokers")))
		return
	}

	if topicPattern == "" && verbose {
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

	kfkClient, err := sarama.NewClient([]string{broker0.Addr()}, sarama.NewConfig())
	if err != nil {
		if verbose {
			this.Ui.Output(color.Yellow("%5s%s %s", " ", broker0.Addr(),
				err.Error()))
		}

		return
	}
	defer kfkClient.Close()

	topics, err := kfkClient.Topics()
	must(err)
	if len(topics) == 0 {
		if topicPattern == "" && verbose {
			this.Ui.Output(fmt.Sprintf("%5s%s", " ", color.Magenta("no topics")))
		}

		return
	}

	for _, topic := range topics {
		if topicPattern != "" {
			if matched, _ := regexp.MatchString(topicPattern, topic); !matched {
				continue
			}
		}

		if verbose {
			this.Ui.Output(strings.Repeat(" ", 4) + color.Blue(topic))
		}

		// get partitions and check if some dead
		alivePartitions, err := kfkClient.WritablePartitions(topic)
		must(err)
		partions, err := kfkClient.Partitions(topic)
		must(err)
		if len(alivePartitions) != len(partions) {
			this.Ui.Output(fmt.Sprintf("topic[%s] has %s partitions: %+v/%+v",
				alivePartitions, color.Red("dead"), partions))
		}

		if !verbose {
			this.Ui.Output(strings.Repeat(" ", 4) + color.Blue("%30s %d",
				topic, len(partions)))
			continue
		}

		for _, partitionID := range alivePartitions {
			leader, err := kfkClient.Leader(topic, partitionID)
			must(err)

			replicas, err := kfkClient.Replicas(topic, partitionID)
			must(err)

			isr, err := kfkClient.Isr(topic, partitionID)
			must(err)

			underReplicated := false
			if len(isr) != len(replicas) {
				underReplicated = true
			}

			latestOffset, err := kfkClient.GetOffset(topic, partitionID,
				sarama.OffsetNewest)
			must(err)

			oldestOffset, err := kfkClient.GetOffset(topic, partitionID,
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

func (this *Topics) addTopic(zkAddrs string, topic string, replicas,
	partitions int) error {
	log.Debug("creating kafka topic: %s", topic)

	cmd := pipestream.New(fmt.Sprintf("%s/bin/kafka-topics.sh", config.KafkaHome()),
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
	var lastLine string
	for scanner.Scan() {
		lastLine = scanner.Text()
		log.Debug(lastLine)
	}
	err = scanner.Err()
	if err != nil {
		return err
	}
	cmd.Close()

	log.Info("kafka created topic[%s]: %s", topic, lastLine)
	return nil
}

func (*Topics) Synopsis() string {
	return "Manage topics & partitions of a zone"
}

func (*Topics) Help() string {
	help := `
Usage: gafka topics -z zone [options]

	Manage topics & partitions of a zone

Options:
  
  -c cluster

  -t topic
  	Topic name pattern to display, regex supported.

  -config k=v
  	Config a topic. TODO

  -add topic
  	Add a topic to a kafka cluster.

  -partitions n

  -replicas n

  -verbose
`
	return strings.TrimSpace(help)
}
