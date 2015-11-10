package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/samuel/go-zookeeper/zk"
)

type Clusters struct {
	Ui cli.Ui
}

func (c *Clusters) Run(args []string) (exitCode int) {

	var (
		addMode     bool
		clusterName string
		clusterPath string
		zone        string
	)
	cmdFlags := flag.NewFlagSet("clusters", flag.ContinueOnError)
	cmdFlags.Usage = func() { c.Ui.Output(c.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.BoolVar(&addMode, "a", false, "")
	cmdFlags.StringVar(&clusterName, "n", "", "")
	cmdFlags.StringVar(&clusterPath, "p", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if !c.validate(addMode, clusterName, clusterPath, zone) {
		return 1
	}

	zkConn := zkConnect(cf.Zones[zone])

	if !addMode {
		for name, path := range getClusters(zkConn) {
			c.Ui.Output(fmt.Sprintf("%20s: %s", name, path))
		}
		return
	}

	// add cluster
	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	_, err := zkConn.Create(clusterRoot+clusterName, []byte(clusterPath), flags, acl)
	switch err {
	case nil:
	case zk.ErrNodeExists:
		c.Ui.Error(fmt.Sprintf("cluster: %s already exists", clusterName))
		return 2
	default:
		c.Ui.Error(err.Error())
		return 3
	}

	return
}

func (c *Clusters) validate(addMode bool, name, path string, zone string) bool {
	if zone != "" {
		if _, present := cf.Zones[zone]; !present {
			c.Ui.Error(fmt.Sprintf("invalid zone: %s", zone))
			return false
		}
	}

	if addMode {
		if name == "" || zone == "" || path == "" {
			// TODO more strict validator on clusterName
			c.Ui.Error("when add new cluster, you must specify zone, cluster name, cluster path")
			return false
		}

	}

	return true
}

func (c *Clusters) Synopsis() string {
	return "Print available kafka clusters from Zookeeper"
}

func (c *Clusters) Help() string {
	help := `
Usage: gafka clusters [options]

	Print available kafka clusters from Zookeeper

Options:

  -z, --zone
  	Only print kafka clusters within this zone.

  -a, --add
  	Add a new kafka cluster into a zone.

  -n, --name
  	The new kafka cluster name.

`
	return strings.TrimSpace(help)
}
