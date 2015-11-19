package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Discover struct {
	Ui  cli.Ui
	Cmd string
}

// TODO cluster info will contain desciption,owner,etc.
func (this *Discover) Run(args []string) (exitCode int) {
	var (
		zone string
	)
	cmdFlags := flag.NewFlagSet("discover", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone != "" {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
		this.discoverClusters(zkzone)
	} else {
		forAllZones(func(zkzone *zk.ZkZone) {
			this.discoverClusters(zkzone)
		})
	}

	return
}

func (this *Discover) discoverClusters(zkzone *zk.ZkZone) {
	this.Ui.Output(zkzone.Name())

	existingClusters := zkzone.Clusters()
	existingCluserPaths := make(map[string]struct{}, len(existingClusters))
	for _, path := range existingClusters {
		existingCluserPaths[path] = struct{}{}
	}

	clusters, err := zkzone.DiscoverClusters("/")
	if err != nil {
		this.Ui.Error(zkzone.Name() + ": " + err.Error())
		return
	}

	for _, zkpath := range clusters {
		if _, present := existingCluserPaths[zkpath]; !present {
			this.Ui.Output(strings.Repeat(" ", 4) + color.Yellow(zkpath))
		} else {
			this.Ui.Output(strings.Repeat(" ", 4) + zkpath)
		}
	}
}

func (*Discover) Synopsis() string {
	return "Automatically discover online kafka clusters"
}

func (this *Discover) Help() string {
	help := fmt.Sprintf(`
Usage: %s discover [options]

	Automatically discover online kafka clusters

Options:

  -z zone
  	Only print kafka clusters within this zone.

  

`, this.Cmd)
	return strings.TrimSpace(help)
}
