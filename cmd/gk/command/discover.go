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
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		this.discoverClusters(zkzone)
	} else {
		forSortedZones(func(zkzone *zk.ZkZone) {
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

	discoveredClusters, err := zkzone.DiscoverClusters("/")
	if err != nil {
		this.Ui.Error(zkzone.Name() + ": " + err.Error())
		return
	}

	// print each cluster state: new, normal
	for _, zkpath := range discoveredClusters {
		if _, present := existingCluserPaths[zkpath]; !present {
			this.Ui.Output(strings.Repeat(" ", 4) + color.Green("%s +++",
				zkpath))
		} else {
			this.Ui.Output(strings.Repeat(" ", 4) + zkpath)
		}
	}

	// find the offline clusters
	for c, path := range existingClusters {
		path = strings.TrimSpace(path)
		foundOnline := false
		for _, p := range discoveredClusters {
			p = strings.TrimSpace(p)
			if p == path {
				foundOnline = true
				break
			}
		}
		if !foundOnline {
			this.Ui.Output(strings.Repeat(" ", 4) + color.Red("%s: %s ---", c, path))
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

`, this.Cmd)
	return strings.TrimSpace(help)
}
