package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/sarama"
)

type UnderReplicated struct {
	Ui  cli.Ui
	Cmd string
}

func (this *UnderReplicated) Run(args []string) (exitCode int) {
	if len(args) == 0 {
		this.Ui.Error("zone required")
		this.Ui.Output(this.Help())
		return 2
	}

	for _, zone := range args {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
		zkzone.WithinClusters(func(cluster string, path string) {
			zkcluster := zkzone.NewCluster(cluster)
			this.displayUnderReplicatedPartitionsOfCluster(zkcluster)
		})
	}

	return
}

func (this *UnderReplicated) swallow(err error) {
	if err != nil {
		panic(err)
	}
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
		this.Ui.Output(color.Yellow("%+v%s %s", " ", brokerList, err.Error()))

		return
	}
	defer kfk.Close()

	topics, err := kfk.Topics()
	this.swallow(err)
	if len(topics) == 0 {
		return
	}

	for _, topic := range topics {
		// get partitions and check if some dead
		alivePartitions, err := kfk.WritablePartitions(topic)
		this.swallow(err)
		partions, err := kfk.Partitions(topic)
		this.swallow(err)
		if len(alivePartitions) != len(partions) {
			this.Ui.Output(fmt.Sprintf("topic[%s] has %s partitions: %+v/%+v",
				alivePartitions, color.Red("dead"), partions))
		}

		for _, partitionID := range alivePartitions {
			replicas, err := kfk.Replicas(topic, partitionID)
			this.swallow(err)

			isr := zkcluster.Isr(topic, partitionID)

			underReplicated := false
			if len(isr) != len(replicas) {
				underReplicated = true
			}

			if underReplicated {
				leader, err := kfk.Leader(topic, partitionID)
				this.swallow(err)

				latestOffset, err := kfk.GetOffset(topic, partitionID,
					sarama.OffsetNewest)
				this.swallow(err)

				oldestOffset, err := kfk.GetOffset(topic, partitionID,
					sarama.OffsetOldest)
				this.swallow(err)

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
Usage: %s underreplicated [zone ...]

	Display under-replicated partitions
`, this.Cmd)
	return strings.TrimSpace(help)
}
