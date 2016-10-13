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
	"github.com/ryanuber/columnize"
)

type Lags struct {
	Ui  cli.Ui
	Cmd string

	onlineOnly      bool
	groupPattern    string
	topicPattern    string
	watchMode       bool
	problematicMode bool
	tableFmt        bool
	lagThreshold    int
	lagTotal        int64
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
	cmdFlags.BoolVar(&this.tableFmt, "table", false, "")
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
				if this.tableFmt {
					this.printConsumersLagTable(zkcluster)
				} else {
					this.printConsumersLag(zkcluster)
				}
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

		this.Ui.Output(fmt.Sprintf("Lag totals: %s", gofmt.Comma(this.lagTotal)))

		return
	}

	// display a single cluster
	for {
		zkcluster := zkzone.NewCluster(cluster) // panic if invalid cluster
		if this.tableFmt {
			this.printConsumersLagTable(zkcluster)
		} else {
			this.printConsumersLag(zkcluster)
		}

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

	this.Ui.Output(fmt.Sprintf("Lag totals: %s", gofmt.Comma(this.lagTotal)))

	return
}

func (this *Lags) printConsumersLagTable(zkcluster *zk.ZkCluster) {
	lines := make([]string, 0)
	header := "ConsumerGroup|Topic/Partition|Produced|Consumed|Lag|Committed|Uptime"
	lines = append(lines, header)

	// sort by group name
	consumersByGroup := zkcluster.ConsumersByGroup(this.groupPattern)
	sortedGroups := make([]string, 0, len(consumersByGroup))
	for group, _ := range consumersByGroup {
		sortedGroups = append(sortedGroups, group)
	}
	sort.Strings(sortedGroups)

	for _, group := range sortedGroups {
		if !patternMatched(group, this.groupPattern) {
			continue
		}

		sortedTopicAndPartitionIds := make([]string, 0, len(consumersByGroup[group]))
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
			if !consumer.Online {
				continue
			}
			if this.problematicMode && consumer.Lag <= int64(this.lagThreshold) {
				continue
			}
			if consumer.ConsumerZnode == nil {
				this.Ui.Warn(fmt.Sprintf("%+v has no znode", consumer))
				continue
			}

			this.lagTotal += consumer.Lag

			lines = append(lines,
				fmt.Sprintf("%s|%s/%s|%s|%s|%s|%s|%s",
					group,
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					gofmt.Comma(consumer.Lag),
					gofmt.PrettySince(consumer.Mtime.Time()),
					gofmt.PrettySince(consumer.ConsumerZnode.Uptime())))
		}
	}

	if len(lines) > 1 {
		this.Ui.Info(fmt.Sprintf("%s ▾", zkcluster.Name()))
		this.Ui.Output(columnize.SimpleFormat(lines))
	}
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
		lines := make([]string, 0, 100)

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
					symbol = color.Yellow("◎")
				}
			} else {
				lagOutput = color.Blue("%15s", gofmt.Comma(consumer.Lag))
				if consumer.Online {
					symbol = color.Green("◉")
				} else {
					symbol = color.Yellow("◎")
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
					host = color.Green("%s", consumer.ConsumerZnode.Host())
					if time.Since(consumer.ConsumerZnode.Uptime()) < time.Hour {
						uptime = color.Magenta(gofmt.PrettySince(consumer.ConsumerZnode.Uptime()))
					} else {
						uptime = gofmt.PrettySince(consumer.ConsumerZnode.Uptime())
					}
				}

				this.lagTotal += consumer.Lag

				lines = append(lines, fmt.Sprintf("\t%s %35s/%-2s %12s -> %-15s %s %-10s %s %s",
					symbol,
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					lagOutput,
					gofmt.PrettySince(consumer.Mtime.Time()),
					host, uptime))
			} else if !this.onlineOnly {
				lines = append(lines, fmt.Sprintf("\t%s %35s/%-2s %12s -> %-12s %s %s",
					symbol,
					consumer.Topic, consumer.PartitionId,
					gofmt.Comma(consumer.ProducerOffset),
					gofmt.Comma(consumer.ConsumerOffset),
					lagOutput,
					gofmt.PrettySince(consumer.Mtime.Time())))
			}
		}

		if len(lines) > 0 {
			this.Ui.Output(strings.Repeat(" ", 4) + group)
			for _, l := range lines {
				this.Ui.Output(l)
			}
		}
	}
}

func (*Lags) Synopsis() string {
	return "Display online high level consumers lag on a topic"
}

func (this *Lags) Help() string {
	help := fmt.Sprintf(`
Usage: %s lags [options]

    %s

Options:

    -z zone
      Default %s

    -c cluster

    -g group name pattern

    -t topic name pattern

    -w
      Watch mode. Keep printing out lags with 60s interval refresh.

    -l
      Only show online consumers lag.

    -lag threshold
      Default 5000.

    -p
      Only show problematic consumers.

    -table
      Display in table format.

`, this.Cmd, this.Synopsis(), ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
