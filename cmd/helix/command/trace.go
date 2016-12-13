package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/model"
	"github.com/funkygao/gocli"
)

type Trace struct {
	Ui  cli.Ui
	Cmd string

	manager helix.HelixManager
	cluster string
}

func (this *Trace) Run(args []string) (exitCode int) {
	var (
		zone string
	)
	cmdFlags := flag.NewFlagSet("trace", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.cluster == "" {
		this.Ui.Error("-c required")
		return 2
	}

	this.manager = getConnectedManager(zone, this.cluster, helix.InstanceTypeSpectator)
	defer this.manager.Disconnect()

	this.startTracing()

	select {}

	return
}

func (this *Trace) startTracing() {
	this.manager.AddExternalViewChangeListener(func(externalViews []*model.Record, context *helix.Context) {

	})
	this.manager.AddLiveInstanceChangeListener(func(liveInstances []*model.Record, context *helix.Context) {

	})

}

func (*Trace) Synopsis() string {
	return "Trace all events within a cluster"
}

func (this *Trace) Help() string {
	help := fmt.Sprintf(`
Usage: %s trace [options]

    %s

Options:

    -z zone

    -c cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
