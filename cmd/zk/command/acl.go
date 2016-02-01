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

type Acl struct {
	Ui  cli.Ui
	Cmd string

	zone string
	path string
}

func (this *Acl) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("acl", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	this.path = args[len(args)-1]

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()
	conn := zkzone.Conn()

	acls, _, err := conn.GetACL(this.path)
	must(err)
	this.Ui.Output(fmt.Sprintf("%# v", pretty.Formatter(acls)))
	return
}

func (*Acl) Synopsis() string {
	return "Show znode ACL info"
}

func (this *Acl) Help() string {
	help := fmt.Sprintf(`
Usage: %s acl [options] path

    Show znode ACL info

Options:

    -z zone

`, this.Cmd)
	return strings.TrimSpace(help)
}
