package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Consumers struct {
	Ui cli.Ui
}

func (this *Consumers) Run(args []string) (exitCode int) {
	var (
		cluster string
		zone    string
	)
	cmdFlags := flag.NewFlagSet("consumers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "") // TODO not used
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" {
		forAllZones(func(zone string, zkzone *zk.ZkZone) {
			this.printConsumers(zone, zkzone, cluster)
		})

		return
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
	this.printConsumers(zone, zkzone, cluster)

	return
}

// Print all controllers of all clusters within a zone.
func (this *Consumers) printConsumers(zone string, zkzone *zk.ZkZone, cluster string) {
	this.Ui.Output(zone)
	zkzone.WithinClusters(func(name, path string) {
		if cluster != "" && cluster != name {
			return
		}

		zkcluster := zkzone.NewCluster(name)
		this.Ui.Output(strings.Repeat(" ", 4) + name)
		for _, consumer := range zkcluster.Consumers() {
			this.Ui.Output(fmt.Sprintf("\t%s", consumer))
		}

	})

}

func (*Consumers) Synopsis() string {
	return "Print online consumers"
}

func (*Consumers) Help() string {
	help := `
Usage: gafka consumers [options]

	Print online consumers

Options:

  -z zone
  	Only print kafka controllers within this zone.

  -c cluster

`
	return strings.TrimSpace(help)
}
