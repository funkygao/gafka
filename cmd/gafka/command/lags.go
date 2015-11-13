package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Lags struct {
	Ui         cli.Ui
	onlineOnly bool
}

func (this *Lags) Run(args []string) (exitCode int) {
	var (
		cluster string
		zone    string
	)
	cmdFlags := flag.NewFlagSet("lags", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&this.onlineOnly, "l", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).require("-z").invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
	if cluster == "" {
		zkzone.WithinClusters(func(cluster, path string) {
			this.Ui.Output(cluster)
			zkcluster := zkzone.NewCluster(cluster)
			this.printConsumersLag(zkcluster)
		})

		return
	}

	this.Ui.Output(cluster)
	zkcluster := zkzone.NewCluster(cluster)
	this.printConsumersLag(zkcluster)
	return
}

func (this *Lags) printConsumersLag(zkcluster *zk.ZkCluster) {
	// sort by group name
	sortedGroups := make([]string, 0)
	consumersByGroup := zkcluster.ConsumersByGroup()
	for group, _ := range consumersByGroup {
		sortedGroups = append(sortedGroups, group)
	}
	sort.Strings(sortedGroups)

	for _, group := range sortedGroups {
		this.Ui.Output(strings.Repeat(" ", 4) + group)
		for _, consumer := range consumersByGroup[group] {
			if consumer.Online {
				this.Ui.Output(fmt.Sprintf("\t%s %s %s %d %s",
					color.Green("☀︎"),
					consumer.Topic, consumer.PartitionId,
					consumer.Offset,
					color.Cyan("%d", consumer.Lag))) // TODO if lag>1000? red alert
			} else if !this.onlineOnly {
				this.Ui.Output(fmt.Sprintf("\t%s %s %s %d %s",
					color.Yellow("☔︎︎"),
					consumer.Topic, consumer.PartitionId,
					consumer.Offset,
					color.Cyan("%d", consumer.Lag)))
			}
		}
	}
}

func (*Lags) Synopsis() string {
	return "Display consumer lags TODO"
}

func (*Lags) Help() string {
	help := `
Usage: gafka lags -z zone [options]

  -c cluster

  -l
  	Only show online consumers lag.
`
	return strings.TrimSpace(help)
}
