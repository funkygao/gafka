package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Clusters struct {
	Ui  cli.Ui
	Cmd string
}

// TODO cluster info will contain desciption,owner,etc.
func (this *Clusters) Run(args []string) (exitCode int) {
	var (
		addMode     bool
		clusterName string
		clusterPath string
		zone        string
	)
	cmdFlags := flag.NewFlagSet("clusters", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.BoolVar(&addMode, "a", false, "")
	cmdFlags.StringVar(&clusterName, "n", "", "")
	cmdFlags.StringVar(&clusterPath, "p", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).on("-a", "-n", "-z", "-p").invalid(args) {
		return 2
	}

	if !addMode {
		if zone != "" {
			zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
			this.printClusters(zkzone)
		} else {
			// print all zones all clusters
			forAllZones(func(zone string, zkzone *zk.ZkZone) {
				this.printClusters(zkzone)
			})
		}

		return
	}

	// add cluster
	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
	if err := zkzone.RegisterCluster(clusterName, clusterPath); err != nil {
		this.Ui.Error(err.Error())
		return 1
	}

	return
}

func (this *Clusters) printClusters(zkzone *zk.ZkZone) {
	clusters := make([][2]string, 0)
	zkzone.WithinClusters(func(name, path string) {
		clusters = append(clusters, [2]string{name, path})
	})

	this.Ui.Output(fmt.Sprintf("%s: %d", zkzone.Name(), len(clusters)))
	for _, c := range clusters {
		this.Ui.Output(fmt.Sprintf("%30s: %s", c[0], c[1]))
	}

}

func (*Clusters) Synopsis() string {
	return "Register kafka clusters to a zone"
}

func (this *Clusters) Help() string {
	help := fmt.Sprintf(`
Usage: %s clusters [options]

	Register kafka clusters to a zone

Options:

  -z zone
  	Only print kafka clusters within this zone.

  -a
  	Add a new kafka cluster into a zone.

  -n cluster name
  	The new kafka cluster name.

  -p cluster zk path
  	The new kafka cluser chroot path in Zookeeper.

`, this.Cmd)
	return strings.TrimSpace(help)
}
