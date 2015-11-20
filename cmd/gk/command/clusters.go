package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Clusters struct {
	Ui      cli.Ui
	Cmd     string
	verbose bool
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
	cmdFlags.BoolVar(&this.verbose, "l", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).on("-a", "-n", "-z", "-p").invalid(args) {
		return 2
	}

	if !addMode {
		if zone != "" {
			zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
			this.printClusters(zkzone)
		} else {
			// print all zones all clusters
			forAllZones(func(zkzone *zk.ZkZone) {
				this.printClusters(zkzone)
			})
		}

		return
	}

	// add cluster
	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	if err := zkzone.RegisterCluster(clusterName, clusterPath); err != nil {
		this.Ui.Error(err.Error())
		return 1
	}

	this.Ui.Info(fmt.Sprintf("%s: %s created", clusterName, clusterPath))

	return
}

func (this *Clusters) printClusters(zkzone *zk.ZkZone) {
	type clusterInfo struct {
		name, path         string
		topicN, partitionN int
		err                string
	}
	clusters := make([]clusterInfo, 0)
	zkzone.WithinClusters(func(name, path string) {
		ci := clusterInfo{
			name: name,
			path: path,
		}
		if !this.verbose {
			clusters = append(clusters, ci)
			return
		}

		// verbose mode, will calculate topics and partition count
		zkcluster := zkzone.NewclusterWithPath(name, path)
		brokerList := zkcluster.BrokerList()
		if len(brokerList) == 0 {
			ci.err = "no live brokers"
			clusters = append(clusters, ci)
			return
		}
		kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
		if err != nil {
			ci.err = err.Error()
			clusters = append(clusters, ci)
			return
		}
		topics, err := kfk.Topics()
		if err != nil {
			ci.err = err.Error()
			clusters = append(clusters, ci)
			return
		}
		partitionN := 0
		for _, topic := range topics {
			partitions, err := kfk.Partitions(topic)
			if err != nil {
				ci.err = err.Error()
				clusters = append(clusters, ci)
				continue
			}

			partitionN += len(partitions)
		}

		clusters = append(clusters, clusterInfo{
			name:       name,
			path:       path,
			topicN:     len(topics),
			partitionN: partitionN,
		})
	})

	this.Ui.Output(fmt.Sprintf("%s: %d", zkzone.Name(), len(clusters)))
	if this.verbose {
		// 2 loop: 1. print the err clusters 2. print the good clusters
		for _, c := range clusters {
			if c.err == "" {
				continue
			}
			this.Ui.Output(fmt.Sprintf("%30s: %s %s", c.name, c.path,
				color.Red(c.err)))
		}

		for _, c := range clusters {
			if c.err != "" {
				continue
			}

			this.Ui.Output(fmt.Sprintf("%30s: topics:%2d partitions:%3d %s",
				c.name, c.topicN, c.partitionN, c.path))
		}

		return
	}

	// not verbose mode
	for _, c := range clusters {
		this.Ui.Output(fmt.Sprintf("%30s: %s", c.name, c.path))
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

    -l
      Use a long listing format.

    -a
      Add a new kafka cluster into a zone.

    -n cluster name
      The new kafka cluster name.

    -p cluster zk path
      The new kafka cluster chroot path in Zookeeper.

`, this.Cmd)
	return strings.TrimSpace(help)
}
