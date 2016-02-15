package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type UnderReplicated struct {
	Ui  cli.Ui
	Cmd string

	zone    string
	cluster string
}

func (this *UnderReplicated) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("underreplicated", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.zone == "" {
		forSortedZones(func(zkzone *zk.ZkZone) {
			this.Ui.Output(zkzone.Name())
			zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
				this.displayUnderReplicatedPartitionsOfCluster(zkcluster)
			})

			printSwallowedErrors(this.Ui, zkzone)
		})

		return
	}

	// a single zone
	ensureZoneValid(this.zone)
	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	this.Ui.Output(zkzone.Name())
	if this.cluster != "" {
		zkcluster := zkzone.NewCluster(this.cluster)
		this.displayUnderReplicatedPartitionsOfCluster(zkcluster)
	} else {
		// all clusters
		zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
			this.displayUnderReplicatedPartitionsOfCluster(zkcluster)
		})
	}

	printSwallowedErrors(this.Ui, zkzone)

	return
}

func (this *UnderReplicated) displayUnderReplicatedPartitionsOfCluster(zkcluster *zk.ZkCluster) {
	this.Ui.Output(fmt.Sprintf("%s %s", strings.Repeat(" ", 4), zkcluster.Name()))

	brokerList := zkcluster.BrokerList()
	if len(brokerList) == 0 {
		this.Ui.Output(fmt.Sprintf("%4s%s", " ", color.Red("empty brokers")))
		return
	}

	kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		this.Ui.Output(color.Yellow("%4s%+v %s", " ", brokerList, err.Error()))

		return
	}
	defer kfk.Close()

	topics, err := kfk.Topics()
	swallow(err)
	if len(topics) == 0 {
		return
	}

	for _, topic := range topics {
		// get partitions and check if some dead
		alivePartitions, err := kfk.WritablePartitions(topic)
		if err != nil {
			this.Ui.Error(color.Red("topic[%s] cannot fetch writable partitions: %v", topic, err))
			continue
		}
		partions, err := kfk.Partitions(topic)
		if err != nil {
			this.Ui.Error(color.Red("topic[%s] cannot fetch partitions: %v", topic, err))
			continue
		}
		if len(alivePartitions) != len(partions) {
			this.Ui.Error(fmt.Sprintf("topic[%s] has %s partitions: %+v/%+v",
				topic, color.Red("dead"), alivePartitions, partions))
		}

		for _, partitionID := range alivePartitions {
			replicas, err := kfk.Replicas(topic, partitionID)
			if err != nil {
				this.Ui.Error(color.Red("topic[%s] P:%d: %v", topic, partitionID, err))
				continue
			}

			isr := zkcluster.Isr(topic, partitionID)

			underReplicated := false
			if len(isr) != len(replicas) {
				underReplicated = true
			}

			if underReplicated {
				leader, err := kfk.Leader(topic, partitionID)
				swallow(err)

				latestOffset, err := kfk.GetOffset(topic, partitionID, sarama.OffsetNewest)
				swallow(err)

				oldestOffset, err := kfk.GetOffset(topic, partitionID, sarama.OffsetOldest)
				swallow(err)

				this.Ui.Output(color.Red("\t%s Partition:%d Leader:%d Replicas:%+v Isr:%+v Offset:%d Num:%d",
					topic,
					partitionID, leader.ID(), replicas, isr,
					latestOffset, latestOffset-oldestOffset))
			}
		}
	}
}

func (*UnderReplicated) Synopsis() string {
	return "Display under-replicated partitions"
}

func (this *UnderReplicated) Help() string {
	help := fmt.Sprintf(`
Usage: %s underreplicated [options]

    Display under-replicated partitions

Options:

    -z zone
`, this.Cmd)
	return strings.TrimSpace(help)
}
