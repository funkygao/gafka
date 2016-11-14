package command

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Redis struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Redis) Run(args []string) (exitCode int) {
	var (
		zone string
		add  string
		list bool
		del  string
	)
	cmdFlags := flag.NewFlagSet("redis", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&add, "add", "", "")
	cmdFlags.BoolVar(&list, "list", true, "")
	cmdFlags.StringVar(&del, "del", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))

	if add != "" {
		host, port, err := net.SplitHostPort(add)
		swallow(err)

		nport, err := strconv.Atoi(port)
		swallow(err)
		zkzone.AddRedis(host, nport)
	} else if del != "" {
		host, port, err := net.SplitHostPort(add)
		swallow(err)

		nport, err := strconv.Atoi(port)
		swallow(err)
		zkzone.DelRedis(host, nport)
	} else if list {
		for _, hostPort := range zkzone.AllRedis() {
			this.Ui.Output(hostPort)
		}
	}

	return
}

func (*Redis) Synopsis() string {
	return "Manipulate redis instances for kguard"
}

func (this *Redis) Help() string {
	help := fmt.Sprintf(`
Usage: %s redis [options]

    %s

    -z zone

    -list

    -add host:port

    -del host:port

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
