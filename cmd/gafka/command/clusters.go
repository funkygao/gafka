package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Clusters struct {
	Ui cli.Ui
}

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

	if !this.validate(addMode, clusterName, clusterPath, zone) {
		return 1
	}

	zkutil := zk.NewZkUtil(zk.DefaultConfig(cf.Zones[zone]))
	if !addMode {
		for name, path := range zkutil.GetClusters() {
			this.Ui.Output(fmt.Sprintf("%20s: %s", name, path))
		}
		return
	}

	// add cluster
	if err := zkutil.AddCluster(clusterName, clusterPath); err != nil {
		this.Ui.Error(err.Error())
		return 1
	}

	return
}

func (this *Clusters) validate(addMode bool, name, path string, zone string) bool {
	if zone != "" {
		if _, present := cf.Zones[zone]; !present {
			this.Ui.Error(fmt.Sprintf("invalid zone: %s", zone))
			return false
		}
	}

	if addMode {
		if name == "" || zone == "" || path == "" {
			// TODO more strict validator on clusterName
			this.Ui.Error("when add new cluster, you must specify zone, cluster name, cluster path")
			return false
		}

	}

	return true
}

func (*Clusters) Synopsis() string {
	return "Print available kafka clusters from Zookeeper"
}

func (*Clusters) Help() string {
	help := `
Usage: gafka clusters [options]

	Print available kafka clusters from Zookeeper

Options:

  -z
  	Only print kafka clusters within this zone.

  -a
  	Add a new kafka cluster into a zone.

  -n
  	The new kafka cluster name.

  -p
  	The new kafka cluser chroot path in Zookeeper.

`
	return strings.TrimSpace(help)
}
