package command

import (
	"flag"
	"fmt"
	"net"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/gocli"
)

type Node struct {
	Ui  cli.Ui
	Cmd string

	admin helix.HelixAdmin
}

func (this *Node) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
		add     string
		drop    string
	)
	cmdFlags := flag.NewFlagSet("node", flag.ContinueOnError)
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&add, "add", "", "")
	cmdFlags.StringVar(&drop, "drop", "", "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if cluster == "" {
		this.Ui.Error("-c required")
		return 2
	}

	this.admin = getConnectedAdmin(zone)
	defer this.admin.Disconnect()

	switch {
	case add != "":
		host, port, err := net.SplitHostPort(add)
		must(err)
		node := fmt.Sprintf("%s_%s", host, port)
		must(this.admin.AddNode(cluster, node))

	case drop != "":
		host, port, err := net.SplitHostPort(drop)
		must(err)
		node := fmt.Sprintf("%s_%s", host, port)
		must(this.admin.DropNode(cluster, node))

	default:
		instances, err := this.admin.Instances(cluster)
		must(err)
		for _, instance := range instances {
			this.Ui.Output(instance)

			// TODO display each instance info
		}
	}

	return
}

func (*Node) Synopsis() string {
	return "Node/Instance/Participant of a cluster management"
}

func (this *Node) Help() string {
	help := fmt.Sprintf(`
Usage: %s node [options]

    %s

Options:

    -z zone

    -c cluster

    -add host:port

    -drop host:port

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
