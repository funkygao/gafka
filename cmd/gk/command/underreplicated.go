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
	Ui   cli.Ui
	Cmd  string
	zone string
}

func (this *UnderReplicated) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("underreplicated", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.zone == "" {
		forAllZones(func(zkzone *zk.ZkZone) {
			zkzone.WithinClusters(func(zkcluster *zk.ZkCluster) {
				this.displayUnderReplicatedPartitionsOfCluster(zkcluster)
			})
		})

		return
	}

	// a single zone
	ensureZoneValid(this.zone)
	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkzone.WithinClusters(func(zkcluster *zk.ZkCluster) {
		this.displayUnderReplicatedPartitionsOfCluster(zkcluster)
	})

	return
}

func (this *UnderReplicated) displayUnderReplicatedPartitionsOfCluster(zkcluster *zk.ZkCluster) {
	this.Ui.Output(zkcluster.Name())

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
		swallow(err)
		partions, err := kfk.Partitions(topic)
		swallow(err)
		if len(alivePartitions) != len(partions) {
			this.Ui.Output(fmt.Sprintf("topic[%s] has %s partitions: %+v/%+v",
				alivePartitions, color.Red("dead"), partions))
		}

		for _, partitionID := range alivePartitions {
			replicas, err := kfk.Replicas(topic, partitionID)
			swallow(err)

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
