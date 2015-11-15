package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
)

type Lags struct {
	Ui         cli.Ui
	Cmd        string
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
	sortedGroups := make([]string, 0)
	consumersByGroup := zkcluster.ConsumersByGroup()
	for group, _ := range consumersByGroup {
		sortedGroups = append(sortedGroups, group)
	}
	sort.Strings(sortedGroups)

	for _, group := range sortedGroups {
		this.Ui.Output(strings.Repeat(" ", 4) + group)
		for _, consumer := range consumersByGroup[group] {
			// TODO if lag>1000? red alert
			if consumer.Online {
				this.Ui.Output(fmt.Sprintf("\t%s %s/%s %s -> %s %s",
					color.Green("☀︎"),
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					color.Cyan("%s", gofmt.Comma(consumer.Lag))))
			} else if !this.onlineOnly {
				this.Ui.Output(fmt.Sprintf("\t%s %s/%s %s -> %s %s",
					color.Yellow("☔︎︎"),
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					color.Cyan("%s", gofmt.Comma(consumer.Lag))))
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

  -l
  	Only show online consumers lag.
`, this.Cmd)
	return strings.TrimSpace(help)
}
