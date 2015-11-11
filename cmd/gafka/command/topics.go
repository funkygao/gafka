package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Topics struct {
	Ui cli.Ui
}

func (this *Topics) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
		topic   string
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&topic, "t", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" {
		this.Ui.Error("empty zone not allowed")
		this.Ui.Output(this.Help())
		return 2
	}

	ensureZoneValid(zone)

	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(config.ZonePath(zone)))
	if cluster != "" {
		zkcluster := zkzone.NewCluster(cluster)
		broker0 := zkcluster.Brokers()["0"]
		kc, err := sarama.NewClient([]string{broker0.Addr()}, sarama.NewConfig())
		must(err)

		topics, err := kc.Topics()
		must(err)
		for _, topic := range topics {
			partions, err := kc.Partitions(topic)
			must(err)

			this.Ui.Output(topic)
			for _, partitionID := range partions {
				leader, err := kc.Leader(topic, partitionID)
				must(err)

				this.Ui.Output(leader.Addr())

				replicas, err := kc.Replicas(topic, partitionID)
				must(err)

				this.Ui.Output(fmt.Sprintf("%4d Leader:%+v Replicas:%+v Isr:%+v",
					partitionID, replicas, leader.ID()))
			}
		}
		kc.Close()

		return
	}

	// all clusters

	return

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
`
	return strings.TrimSpace(help)
}
