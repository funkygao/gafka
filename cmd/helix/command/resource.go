package command

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/gocli"
)

type Resource struct {
	Ui  cli.Ui
	Cmd string

	admin   helix.HelixAdmin
	cluster string
}

func (this *Resource) Run(args []string) (exitCode int) {
	var (
		zone    string
		add     string
		drop    string
		scale   string
		verbose bool
	)
	cmdFlags := flag.NewFlagSet("resource", flag.ContinueOnError)
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&add, "add", "", "")
	cmdFlags.StringVar(&drop, "drop", "", "")
	cmdFlags.StringVar(&scale, "scale", "", "")
	cmdFlags.BoolVar(&verbose, "v", false, "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.cluster == "" {
		this.Ui.Error("-c required")
		return 2
	}

	this.admin = getConnectedAdmin(zone)
	defer this.admin.Disconnect()

	switch {
	case add != "":
		tuple := strings.SplitN(add, ":", 3)
		stateModel := tuple[2]
		partitions, _ := strconv.Atoi(tuple[1])
		must(this.admin.AddResource(this.cluster, tuple[0], helix.DefaultAddResourceOption(partitions, stateModel)))

	case drop != "":
		must(this.admin.DropResource(this.cluster, drop))

	case scale != "":
		// TODO

	default:
		resources, err := this.admin.Resources(this.cluster)
		must(err)

		for _, resource := range resources {
			is, err := this.admin.ResourceIdealState(this.cluster, resource)
			must(err)

			this.Ui.Info(fmt.Sprintf("%s[%s]", resource, is.StateModelDefRef()))

			if verbose {
				this.Ui.Output(fmt.Sprintf("%+v", is))
			}

		}
	}

	return
}

func (*Resource) Synopsis() string {
	return "Resources management of a cluster"
}

func (this *Resource) Help() string {
	help := fmt.Sprintf(`
Usage: %s resource [options]

    %s

Options:

    -z zone

    -c cluster

    -v
      Verbose

    -add resource:partition:stateModel

    -drop resource

    -scale resource:partition

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
