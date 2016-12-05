package command

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/store/zk"
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

	this.manager = zk.NewZKHelixManager(ctx.Zone(zone).ZkHelix, zk.WithSessionTimeout(time.Second*30))
	if err := this.manager.Connect(); err != nil {
		this.Ui.Error(err.Error())
		return 2
	}

	this.startTracing()
	this.manager.Close()

	return
}

func (this *Trace) startTracing() {
	tracer := this.manager.NewSpectator(this.cluster)
	tracer.AddExternalViewChangeListener(func(externalViews []*helix.Record, context *helix.Context) {

	})
	tracer.AddLiveInstanceChangeListener(func(liveInstances []*helix.Record, context *helix.Context) {

	})

	// startup
	if err := tracer.Start(); err != nil {
		this.Ui.Error(err.Error())
		return
	}
	tracer.Close()
}

func (*Trace) Synopsis() string {
	return "Trace all events"
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
