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
	Ui  cli.Ui
	Cmd string

	onlineOnly      bool
	groupPattern    string
	topicPattern    string
	watchMode       bool
	problematicMode bool
	lagThreshold    int
}

func (this *Lags) Run(args []string) (exitCode int) {
	const secondsInMinute = 60
	var (
		cluster string
		zone    string
	)
	cmdFlags := flag.NewFlagSet("lags", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&this.onlineOnly, "l", false, "")
	cmdFlags.BoolVar(&this.problematicMode, "p", false, "")
	cmdFlags.StringVar(&this.groupPattern, "g", "", "")
	cmdFlags.StringVar(&this.topicPattern, "t", "", "")
	cmdFlags.BoolVar(&this.watchMode, "w", false, "")
	cmdFlags.IntVar(&this.lagThreshold, "lag", 5000, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.watchMode {
		refreshScreen()
	}

	if this.problematicMode {
		this.onlineOnly = true
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	bar := progress.New(secondsInMinute)
	if cluster == "" {
		for {
			zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
				this.Ui.Output(zkcluster.Name())
				this.printConsumersLag(zkcluster)
			})

			if this.watchMode {
				for i := 1; i <= secondsInMinute; i++ {
					bar.ShowProgress(i)
					time.Sleep(time.Second)
				}
			} else {
				break
			}

			printSwallowedErrors(this.Ui, zkzone)
		}

		return
	}

	for {
		this.Ui.Output(cluster)
		zkcluster := zkzone.NewCluster(cluster) // panic if invalid cluster
		this.printConsumersLag(zkcluster)

		if this.watchMode {
			for i := 1; i <= secondsInMinute; i++ {
				bar.ShowProgress(i)
				time.Sleep(time.Second)
			}
			//refreshScreen()
		} else {
			break
		}

		printSwallowedErrors(this.Ui, zkzone)
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

		sortedTopicAndPartitionIds := make([]string, 0)
		consumers := make(map[string]zk.ConsumerMeta)
		for _, t := range consumersByGroup[group] {
			key := fmt.Sprintf("%s:%s", t.Topic, t.PartitionId)
			sortedTopicAndPartitionIds = append(sortedTopicAndPartitionIds, key)

			consumers[key] = t
		}
		sort.Strings(sortedTopicAndPartitionIds)

		for _, topicAndPartitionId := range sortedTopicAndPartitionIds {
			consumer := consumers[topicAndPartitionId]

			if !patternMatched(consumer.Topic, this.topicPattern) {
				continue
			}

			var (
				lagOutput string
				symbol    string
			)
			if consumer.Lag > int64(this.lagThreshold) {
				lagOutput = color.Red("%15s", gofmt.Comma(consumer.Lag))
				if consumer.Online {
					symbol = color.Yellow("⚠︎︎")
				} else {
					symbol = color.Yellow("☔︎︎")
				}
			} else {
				lagOutput = color.Blue("%15s", gofmt.Comma(consumer.Lag))
				if consumer.Online {
					symbol = color.Green("☀︎")
				} else {
					symbol = color.Yellow("☔︎︎")
				}
			}

			if consumer.Online {
				if this.problematicMode && consumer.Lag <= int64(this.lagThreshold) {
					continue
				}

				var (
					host   string
					uptime string
				)
				if consumer.ConsumerZnode == nil {
					host = "unrecognized"
					uptime = "-"
				} else {
					host = color.Green("%90s", consumer.ConsumerZnode.Host())
					uptime = gofmt.PrettySince(consumer.ConsumerZnode.Uptime())
				}

				this.Ui.Output(fmt.Sprintf("\t%s %35s/%-2s %12s -> %-12s %s %s\n%s %s",
					symbol,
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					lagOutput,
					gofmt.PrettySince(consumer.Mtime.Time()),
					host, uptime))
			} else if !this.onlineOnly {
				this.Ui.Output(fmt.Sprintf("\t%s %35s/%-2s %12s -> %-12s %s %s",
					symbol,
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
	return "Display high level consumers lag for each topic/partition"
}

func (this *Lags) Help() string {
	help := fmt.Sprintf(`
Usage: %s lags [options]

    Display high level consumers lag for each topic/partition

    e,g.
    %s -z prod -c trade -t OrderStatusMsg

Options:

    -z zone
      Default %s

    -c cluster

    -g consumer group name pattern

    -t topic name pattern

`, this.Cmd, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
