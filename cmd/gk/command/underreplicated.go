package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

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
	debug   bool
}

func (this *UnderReplicated) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("underreplicated", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.BoolVar(&this.debug, "debug", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.debug {
		sarama.Logger = log.New(os.Stderr, color.Magenta("[sarama]"), log.LstdFlags)
	}

	if this.zone == "" {
		forSortedZones(func(zkzone *zk.ZkZone) {
			this.Ui.Output(zkzone.Name())
			zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
				lines := this.displayUnderReplicatedPartitionsOfCluster(zkcluster)
				if len(lines) > 0 {
					this.Ui.Output(fmt.Sprintf("%s %s", strings.Repeat(" ", 4), zkcluster.Name()))
					for _, l := range lines {
						this.Ui.Warn(l)
					}
				}
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
		lines := this.displayUnderReplicatedPartitionsOfCluster(zkcluster)
		if len(lines) > 0 {
			this.Ui.Output(fmt.Sprintf("%s %s", strings.Repeat(" ", 4), zkcluster.Name()))
			for _, l := range lines {
				this.Ui.Warn(l)
			}
		}
	} else {
		// all clusters
		zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
			lines := this.displayUnderReplicatedPartitionsOfCluster(zkcluster)
			if len(lines) > 0 {
				this.Ui.Output(fmt.Sprintf("%s %s", strings.Repeat(" ", 4), zkcluster.Name()))
				for _, l := range lines {
					this.Ui.Warn(l)
				}
			}
		})
	}

	printSwallowedErrors(this.Ui, zkzone)

	return
}

func (this *UnderReplicated) displayUnderReplicatedPartitionsOfCluster(zkcluster *zk.ZkCluster) []string {
	brokerList := zkcluster.BrokerList()
	if len(brokerList) == 0 {
		this.Ui.Warn(fmt.Sprintf("%s empty brokers", zkcluster.Name()))
		return nil
	}

	cf := sarama.NewConfig()
	cf.Net.ReadTimeout = time.Second * 4
	cf.Net.WriteTimeout = time.Second * 4
	kfk, err := sarama.NewClient(brokerList, cf)
	if err != nil {
		this.Ui.Error(fmt.Sprintf("%s %+v %s", zkcluster.Name(), brokerList, err.Error()))

		return nil
	}
	defer kfk.Close()

	topics, err := kfk.Topics()
	swallow(err)
	if len(topics) == 0 {
		return nil
	}

	lines := make([]string, 0, 10)
	for _, topic := range topics {
		// get partitions and check if some dead
		alivePartitions, err := kfk.WritablePartitions(topic)
		if err != nil {
			this.Ui.Error(fmt.Sprintf("%s topic[%s] cannot fetch writable partitions: %v", zkcluster.Name(), topic, err))
			continue
		}
		partions, err := kfk.Partitions(topic)
		if err != nil {
			this.Ui.Error(fmt.Sprintf("%s topic[%s] cannot fetch partitions: %v", zkcluster.Name(), topic, err))
			continue
		}
		if len(alivePartitions) != len(partions) {
			this.Ui.Error(fmt.Sprintf("%s topic[%s] has %s partitions: %+v/%+v", zkcluster.Name(),
				topic, color.Red("dead"), alivePartitions, partions))
		}

		for _, partitionID := range alivePartitions {
			replicas, err := kfk.Replicas(topic, partitionID)
			if err != nil {
				this.Ui.Error(fmt.Sprintf("%s topic[%s] P:%d: %v", zkcluster.Name(), topic, partitionID, err))
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

				lines = append(lines, fmt.Sprintf("\t%s Partition:%d Leader:%d Replicas:%+v Isr:%+v Offset:%d-%d Num:%d",
					topic,
					partitionID, leader.ID(), replicas, isr,
					oldestOffset, latestOffset, latestOffset-oldestOffset))
			}
		}
	}

	return lines
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

    -c cluster

    -debug
`, this.Cmd)
	return strings.TrimSpace(help)
}
