package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/sarama"
)

type UnderReplicated struct {
	Ui cli.Ui
}

func (this *UnderReplicated) Run(args []string) (exitCode int) {
	if len(args) == 0 {
		this.Ui.Error("zone required")
		this.Ui.Output(this.Help())
		return 2
	}

	for _, zone := range args {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
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

	kfkClient, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		this.Ui.Output(color.Yellow("%+v%s %s", " ", brokerList, err.Error()))

		return
	}
	defer kfkClient.Close()

	topics, err := kfkClient.Topics()
	this.swallow(err)
	if len(topics) == 0 {
		return
	}

	for _, topic := range topics {
		// get partitions and check if some dead
		alivePartitions, err := kfkClient.WritablePartitions(topic)
		this.swallow(err)
		partions, err := kfkClient.Partitions(topic)
		this.swallow(err)
		if len(alivePartitions) != len(partions) {
			this.Ui.Output(fmt.Sprintf("topic[%s] has %s partitions: %+v/%+v",
				alivePartitions, color.Red("dead"), partions))
		}

		for _, partitionID := range alivePartitions {
			replicas, err := kfkClient.Replicas(topic, partitionID)
			this.swallow(err)

			isr, err := kfkClient.Isr(topic, partitionID)
			this.swallow(err)

			underReplicated := false
			if len(isr) != len(replicas) {
				underReplicated = true
			}

			if underReplicated {
				leader, err := kfkClient.Leader(topic, partitionID)
				this.swallow(err)

				latestOffset, err := kfkClient.GetOffset(topic, partitionID,
					sarama.OffsetNewest)
				this.swallow(err)

				oldestOffset, err := kfkClient.GetOffset(topic, partitionID,
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

func (*UnderReplicated) Help() string {
	help := `
Usage: gafka underreplicated [zone ...]

	Display under-replicated partitions
`
	return strings.TrimSpace(help)
}
