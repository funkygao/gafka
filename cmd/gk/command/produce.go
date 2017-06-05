package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/stress"
)

type Produce struct {
	Ui  cli.Ui
	Cmd string

	zone, cluster, topic string
	benchMode            bool
	benchmarkMaster      string
	ackAll               bool
	showErr              bool
	zkcluster            *zk.ZkCluster
}

func (this *Produce) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("produce", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.topic, "t", "", "")
	cmdFlags.BoolVar(&this.showErr, "showerr", false, "")
	cmdFlags.BoolVar(&this.ackAll, "ackall", false, "")
	cmdFlags.BoolVar(&this.benchMode, "bench", false, "")
	cmdFlags.StringVar(&this.benchmarkMaster, "master", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-c", "-t").
		requireAdminRights("-t", "-bench").
		invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkcluster := zkzone.NewCluster(this.cluster)
	if this.benchMode {
		this.zkcluster = zkcluster

		stress.Flags.Round = 5
		stress.Flags.Tick = 5
		stress.Flags.C1 = 5
		log.SetOutput(os.Stdout)
		if this.benchmarkMaster != "" {
			stress.Flags.MasterAddr = stress.MasterAddr(this.benchmarkMaster)
		}
		stress.RunStress(this.benchmarkProducer)
		return
	}

	msg, err := this.Ui.Ask("Input>")
	swallow(err)

	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	if this.ackAll {
		cf.Producer.RequiredAcks = sarama.WaitForAll
	}
	p, err := sarama.NewSyncProducer(zkcluster.BrokerList(), cf)
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

func (this *Produce) benchmarkProducer(seq int) {
	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	if this.ackAll {
		cf.Producer.RequiredAcks = sarama.WaitForAll
	}
	p, err := sarama.NewSyncProducer(this.zkcluster.BrokerList(), cf)
	swallow(err)
	defer p.Close()

	msg := []byte(strings.Repeat("X", 1<<10))
	for i := 0; i < 50000; i++ {
		_, _, err = p.SendMessage(&sarama.ProducerMessage{
			Topic: this.topic,
			Value: sarama.ByteEncoder(msg),
		})
		if err != nil {
			stress.IncCounter("fail", 1)
			if this.showErr {
				this.Ui.Error(err.Error())
			}
		} else {
			stress.IncCounter("ok", 1)
		}
	}

}

func (*Produce) Synopsis() string {
	return "Produce a message to specified kafka topic"
}

func (this *Produce) Help() string {
	help := fmt.Sprintf(`
Usage: %s produce [options]

    %s

    -z zone

    -c cluster

    -t topic

    -bench
      Run in benchmark mode.

    -showerr
      Work with benchmark mode to display each error message.

    -ackall
      Replicate to all brokers before reply.

    -master benchmark master address

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
