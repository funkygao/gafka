package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Set struct {
	Ui  cli.Ui
	Cmd string

	zone string
	path string
}

func (this *Set) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("set", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.path, "p", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-p").
		requireAdminRights("-p").
		invalid(args) {
		return 2
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()
	conn := zkzone.Conn()

	version := int32(-1)
	data := inData()
	_, err := conn.Set(this.path, data, version)
	must(err)

	return
}

func (*Set) Synopsis() string {
	return "Write znode data"
}

func (this *Set) Help() string {
	help := fmt.Sprintf(`
Usage: %s set -z zone -p path [options]

    Write znode data

Options:

    e,g. 'echo foo | zk set -z test -p /bar'

`, this.Cmd)
	return strings.TrimSpace(help)
}
