package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
)

type Lags struct {
	Ui           cli.Ui
	Cmd          string
	onlineOnly   bool
	groupPattern string
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
	cmdFlags.StringVar(&this.groupPattern, "g", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).require("-z").invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
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
	consumersByGroup := zkcluster.ConsumersByGroup(this.groupPattern)
	sortedGroups := make([]string, 0, len(consumersByGroup))
	for group, _ := range consumersByGroup {
		sortedGroups = append(sortedGroups, group)
	}
	sort.Strings(sortedGroups)

	for _, group := range sortedGroups {
		this.Ui.Output(strings.Repeat(" ", 4) + group)
		for _, consumer := range consumersByGroup[group] {
			// TODO if lag>1000? red alert
			if consumer.Online {
				this.Ui.Output(fmt.Sprintf("\t%s %35s/%-2s %12s -> %-12s %s %s\n%s %s",
					color.Green("☀︎"),
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					color.Magenta("%15s", gofmt.Comma(consumer.Lag)),
					gofmt.PrettySince(consumer.Mtime.Time()),
					color.Green("%90s", consumer.ConsumerZnode.Host()),
					gofmt.PrettySince(consumer.ConsumerZnode.Uptime())))
			} else if !this.onlineOnly {
				this.Ui.Output(fmt.Sprintf("\t%s %35s/%-2s %12s -> %-12s %s %s",
					color.Yellow("☔︎︎"),
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					color.Magenta("%15s", gofmt.Comma(consumer.Lag)),
					gofmt.PrettySince(consumer.Mtime.Time())))
			}
		}
	}
}

func (*Lags) Synopsis() string {
	return "Display consumers lag for each topic each partition"
}

func (this *Lags) Help() string {
	help := fmt.Sprintf(`
Usage: %s lags -z zone [options]

	Display consumers lag for each topic each partition

  -c cluster

  -g group name pattern

  -l
  	Only show online consumers lag.
`, this.Cmd)
	return strings.TrimSpace(help)
}
