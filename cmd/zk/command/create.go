package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/samuel/go-zookeeper/zk"
)

type Create struct {
	Ui  cli.Ui
	Cmd string

	zone string
	path string
}

func (this *Create) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("create", flag.ContinueOnError)
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

	data := inData()
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	_, err := conn.Create(this.path, data, flags, acl)
	must(err)

	return
}

func (*Create) Synopsis() string {
	return "Create znode with initial data"
}

func (this *Create) Help() string {
	help := fmt.Sprintf(`
Usage: %s create -z zone -p path

    Create znode with initial data

Options:

    e,g. 'echo foo | zk create -z test -p /bar'

`, this.Cmd)
	return strings.TrimSpace(help)
}
