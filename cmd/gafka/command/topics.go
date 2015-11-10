package command

import (
	"flag"
	"strings"

	"github.com/Shopify/sarama"
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

	zkutil := zk.NewZkUtil(zk.DefaultConfig(cf.Zones[zone]))
	if cluster != "" {
		broker0 := zkutil.GetBrokersOfCluster(cluster)["0"]
		b := sarama.NewBroker(broker0.Addr())
		b.GetMetadata(request)

		return
	}

	// cluster provided
	for cluster, brokers := range zkutil.GetBrokers() {
		this.Ui.Output(cluster)
		for brokerId, broker := range brokers {
			this.Ui.Output(fmt.Sprintf("\t%8s %s", brokerId, broker))
		}
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

  -t topic
  	Topic name, regex supported.
`
	return strings.TrimSpace(help)
}
