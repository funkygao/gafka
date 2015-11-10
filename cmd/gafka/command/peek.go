package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Peek struct {
	Ui cli.Ui
}

func (this *Peek) Run(args []string) (exitCode int) {
	var (
		cluster   string
		zone      string
		topic     string
		partition int
	)
	cmdFlags := flag.NewFlagSet("peek", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&topic, "t", "", "")
	cmdFlags.IntVar(&partition, "p", 0, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	ensureZoneValid(zone)

	sarama.NewConsumer(addrs, sarama.NewConfig())

	return
}

func (*Peek) Synopsis() string {
	return "Peek kafka cluster messages ongoing"
}

func (*Peek) Help() string {
	help := `
Usage: gafka peek [options]

	Peek kafka cluster messages ongoing

Options:

  -z zone
  	Only print kafka controllers within this zone.

  -c cluster name

  -t topic name

  -p partition id
`
	return strings.TrimSpace(help)
}
