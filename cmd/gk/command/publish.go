package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Publish struct {
	Ui  cli.Ui
	Cmd string

	zone, cluster, topic string
}

func (this *Publish) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("publish", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.topic, "t", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-c", "-t").
		requireAdminRights("-t").
		invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkcluster := zkzone.NewCluster(this.cluster)

	msg, err := this.Ui.Ask("Input>")
	swallow(err)

	p, err := sarama.NewSyncProducer(zkcluster.BrokerList(), sarama.NewConfig())
	swallow(err)
	defer p.Close()

	partition, offset, err := p.SendMessage(&sarama.ProducerMessage{
		Topic: this.topic,
		Value: sarama.StringEncoder(msg),
	})
	if err != nil {
		this.Ui.Error(err.Error())
		return 1
	}

	this.Ui.Output(fmt.Sprintf("ok, partition:%d, offset:%d", partition, offset))

	return
}

func (*Publish) Synopsis() string {
	return "Publish a message to specified kafka topic"
}

func (this *Publish) Help() string {
	help := fmt.Sprintf(`
Usage: %s publish -z zone [options]

    Publish a message to specified kafka topic

    -z zone

    -c cluster

    -t topic

`, this.Cmd)
	return strings.TrimSpace(help)
}
