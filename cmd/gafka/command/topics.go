package command

import (
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
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
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&topicPattern, "t", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&verbose, "verbose", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" {
		this.Ui.Error("empty zone not allowed")
		this.Ui.Output(this.Help())
		return 2
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

	if topicPattern == "" && verbose {
		this.Ui.Output(fmt.Sprintf("%80d topics", len(topics)))
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

			latestOffset, err := kfkClient.GetOffset(topic, partitionID,
				sarama.OffsetNewest)
			must(err)

			oldestOffset, err := kfkClient.GetOffset(topic, partitionID,
				sarama.OffsetOldest)
			must(err)

			isr := zkcluster.Isr(topic, partitionID)
			underReplicated := false
			if len(isr) != len(replicas) {
				underReplicated = true
			}

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

func (*Topics) Synopsis() string {
	return "Print available topics from Zookeeper"
}

func (*Topics) Help() string {
	help := `
Usage: gafka topics -z zone [options]

	Print available kafka topics from Zookeeper

Options:
  
  -c cluster

  -t topic
  	Topic name, regex supported.

  -verbose
`
	return strings.TrimSpace(help)
}
