package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/pretty"
)

type Stat struct {
	Ui  cli.Ui
	Cmd string

	zone string
	path string
}

func (this *Stat) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("stat", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.path, "p", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-p").
		invalid(args) {
		return 2
	}

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()
	conn := zkzone.Conn()

	_, stat, err := conn.Get(this.path)
	must(err)
	this.Ui.Output(fmt.Sprintf("%# v", pretty.Formatter(*stat)))
	return
}

func (*Stat) Synopsis() string {
	return "Show znode status info"
}

func (this *Stat) Help() string {
	help := fmt.Sprintf(`
Usage: %s create -z zone -p path [options]

    Show znode status info

`, this.Cmd)
	return strings.TrimSpace(help)
}
