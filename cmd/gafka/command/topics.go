package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
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
		return 2
	}

	ensureZoneValid(zone)

	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	zkutil := zk.NewZkUtil(zk.DefaultConfig(cf.Zones[zone]))
	if cluster != "" {
		broker0 := zkutil.GetBrokersOfCluster(cluster)["0"]
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

				replicas, err := kc.Replicas(topic, partitionID)
				must(err)

				req := sarama.MetadataRequest{Topics: []string{"foobar"}}
				x, _ := leader.GetMetadata(&req)
				fmt.Printf("%+v\n", x.Topics[0].Partitions[0])

				this.Ui.Output(fmt.Sprintf("%4d Leader:%+v Replicas:%+v Isr:%+v",
					partitionID, replicas, leader.ID()))
			}
		}
		kc.Close()

		return
	}

	return

}

func (*Topics) Synopsis() string {
	return "Print available topics from Zookeeper"
}

func (*Topics) Help() string {
	help := `
Usage: gafka topics [options]

	Print available kafka topics from Zookeeper

Options:

  -z zone
  	Only print kafka topics within this zone.

  -c cluster

  -t topic
  	Topic name, regex supported.
`
	return strings.TrimSpace(help)
}
