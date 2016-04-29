package command

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Offset struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Offset) Run(args []string) (exitCode int) {
	var (
		zone      string
		cluster   string
		topic     string
		group     string
		partition string
		offset    int64
	)
	cmdFlags := flag.NewFlagSet("offset", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&topic, "t", "", "")
	cmdFlags.StringVar(&group, "g", "", "")
	cmdFlags.Int64Var(&offset, "offset", -1, "")
	cmdFlags.StringVar(&partition, "p", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-c", "-t", "-g", "-p", "-offset").
		requireAdminRights("-z").
		invalid(args) {
		return 2
	}

	if offset < 0 {
		this.Ui.Error("offset must be positive")
		return
	}

	if p, err := strconv.Atoi(partition); err != nil {
		this.Ui.Error("invalid partition")
		return
	} else if p < 0 || p > 100 {
		this.Ui.Error("invalid partition")
		return
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	zkcluster := zkzone.NewCluster(cluster)
	zkcluster.ResetConsumerGroupOffset(topic, group, partition, offset)
	this.Ui.Output("done")
	return
}

func (*Offset) Synopsis() string {
	return "Manually set consumer group offset"
}

func (this *Offset) Help() string {
	help := fmt.Sprintf(`
Usage: %s offset -z zone -c cluster -t topic -g group -p partition -offset offset

    Manually set consumer group offset

`, this.Cmd)
	return strings.TrimSpace(help)
}
