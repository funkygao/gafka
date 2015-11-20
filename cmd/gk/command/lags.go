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
	"github.com/funkygao/golib/progress"
)

type Lags struct {
	Ui           cli.Ui
	Cmd          string
	onlineOnly   bool
	groupPattern string
	topicPattern string
	watchMode    bool
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
	cmdFlags.StringVar(&this.topicPattern, "t", "", "")
	cmdFlags.BoolVar(&this.watchMode, "w", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).require("-z").invalid(args) {
		return 2
	}

	if this.watchMode {
		refreshScreen()
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
	bar := progress.New(60)
	if cluster == "" {
		for {
			zkzone.WithinClusters(func(cluster, path string) {
				this.Ui.Output(cluster)
				zkcluster := zkzone.NewCluster(cluster)
				this.printConsumersLag(zkcluster)
			})

			if this.watchMode {
				for i := 0; i < 60; i++ {
					bar.ShowProgress(i)
					time.Sleep(time.Second)
				}
			} else {
				break
			}
		}

		return
	}

	for {
		this.Ui.Output(cluster)
		zkcluster := zkzone.NewCluster(cluster) // panic if invalid cluster
		this.printConsumersLag(zkcluster)

		if this.watchMode {
			for i := 0; i < 60; i++ {
				bar.ShowProgress(i)
				time.Sleep(time.Second)
			}
		} else {
			break
		}
	}

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
			// TODO sort by partitionId
			if consumer.Online {
				if this.topicPattern != "" && !strings.Contains(consumer.Topic, this.topicPattern) {
					continue
				}

				var lagOutput string
				if consumer.Lag > 1000 {
					lagOutput = color.Red("%15s", gofmt.Comma(consumer.Lag))
				} else {
					lagOutput = color.Magenta("%15s", gofmt.Comma(consumer.Lag))
				}

				this.Ui.Output(fmt.Sprintf("\t%s %35s/%-2s %12s -> %-12s %s %s\n%s %s",
					color.Green("☀︎"),
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					lagOutput,
					gofmt.PrettySince(consumer.Mtime.Time()),
					color.Green("%90s", consumer.ConsumerZnode.Host()),
					gofmt.PrettySince(consumer.ConsumerZnode.Uptime())))
			} else if !this.onlineOnly {
				if this.topicPattern != "" && !strings.Contains(consumer.Topic, this.topicPattern) {
					continue
				}

				var lagOutput string
				if consumer.Lag > 1000 {
					lagOutput = color.Red("%15s", gofmt.Comma(consumer.Lag))
				} else {
					lagOutput = color.Magenta("%15s", gofmt.Comma(consumer.Lag))
				}

				this.Ui.Output(fmt.Sprintf("\t%s %35s/%-2s %12s -> %-12s %s %s",
					color.Yellow("☔︎︎"),
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					lagOutput,
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

Options:

    -c cluster

    -g group name pattern

    -t topic name pattern

    -w
      Watch mode. Keep printing out lags with 60s interval refresh.

    -l
      Only show online consumers lag.

`, this.Cmd)
	return strings.TrimSpace(help)
}
