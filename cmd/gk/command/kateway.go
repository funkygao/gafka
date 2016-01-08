package command

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	zkr "github.com/funkygao/gafka/registry/zk"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
)

type Kateway struct {
	Ui  cli.Ui
	Cmd string

	zone string
}

func (this *Kateway) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("kateway", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	instances, _, err := zkzone.Conn().Children(zkr.Root(this.zone))
	if err != nil {
		if err.Error() == "zk: node does not exist" {
			this.Ui.Output("no kateway running")
			return
		} else {
			swallow(err)
		}
	}

	for _, instance := range instances {
		data, stat, err := zkzone.Conn().Get(zkr.Root(this.zone) + "/" + instance)
		swallow(err)

		info := make(map[string]string)
		json.Unmarshal(data, &info)

		this.Ui.Info(fmt.Sprintf("%s id:%-2s up:%s", info["host"], instance,
			gofmt.PrettySince(zk.ZkTimestamp(stat.Ctime).Time())))
		this.Ui.Output(fmt.Sprintf("    ver: %s\n    build: %s\n    pub: %s\n    sub: %s\n    man: %s\n    dbg: %s",
			info["ver"],
			info["build"],
			info["pub"],
			info["sub"],
			info["man"],
			info["debug"],
		))

	}

	return
}

func (*Kateway) Synopsis() string {
	return "List online kateway instances"
}

func (this *Kateway) Help() string {
	help := fmt.Sprintf(`
Usage: %s kateway [options]

    List online kateway instances

Options:

    -z zone

`, this.Cmd)
	return strings.TrimSpace(help)
}
