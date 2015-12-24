package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Get struct {
	Ui  cli.Ui
	Cmd string

	zone string
	path string
}

func (this *Get) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("get", flag.ContinueOnError)
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

	data, _, err := conn.Get(this.path)
	must(err)

	if len(data) == 0 {
		this.Ui.Output("empty znode")
		return
	}

	this.Ui.Output("raw data:")
	fmt.Println(data)
	this.Ui.Output("string data:")
	this.Ui.Output(string(data)) // TODO what if binary?

	return
}

func (*Get) Synopsis() string {
	return "Show znode data"
}

func (this *Get) Help() string {
	help := fmt.Sprintf(`
Usage: %s get -z zone -p path

    Show znode data

`, this.Cmd)
	return strings.TrimSpace(help)
}
