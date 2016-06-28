package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
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
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	if len(args) == 0 {
		this.Ui.Error("missing path")
		return 2
	}

	this.path = args[len(args)-1]

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()
	conn := zkzone.Conn()

	_, stat, err := conn.Get(this.path)
	must(err)
	this.Ui.Output(fmt.Sprintf("%# v", pretty.Formatter(*stat)))
	ctime := gzk.ZkTimestamp(stat.Ctime).Time()
	mtime := gzk.ZkTimestamp(stat.Mtime).Time()
	this.Ui.Output(fmt.Sprintf("ctime: %s, mtime: %s",
		gofmt.PrettySince(ctime), gofmt.PrettySince(mtime)))
	return
}

func (*Stat) Synopsis() string {
	return "Show znode status info"
}

func (this *Stat) Help() string {
	help := fmt.Sprintf(`
Usage: %s stat [options] <path>

    Show znode status info

Options:

    -z zone

`, this.Cmd)
	return strings.TrimSpace(help)
}
