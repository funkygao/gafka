package command

import (
	"flag"
	"fmt"
	"net"
	"sort"
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
		zone   string
		add    string
		list   bool
		byHost bool
		del    string
	)
	cmdFlags := flag.NewFlagSet("redis", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&add, "add", "", "")
	cmdFlags.BoolVar(&list, "list", true, "")
	cmdFlags.BoolVar(&byHost, "host", false, "")
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
		host, port, err := net.SplitHostPort(del)
		swallow(err)

		nport, err := strconv.Atoi(port)
		swallow(err)
		zkzone.DelRedis(host, nport)
	} else if list {
		machineMap := make(map[string]struct{})
		var machines []string
		hostPorts := zkzone.AllRedis()
		sort.Strings(hostPorts)
		for _, hp := range hostPorts {
			host, port, _ := net.SplitHostPort(hp)
			ips, _ := net.LookupIP(host)
			if _, present := machineMap[ips[0].String()]; !present {
				machineMap[ips[0].String()] = struct{}{}

				machines = append(machines, ips[0].String())
			}
			if !byHost {
				this.Ui.Output(fmt.Sprintf("%35s %s", host, port))
			}

		}

		if byHost {
			sort.Strings(machines)
			for _, ip := range machines {
				this.Ui.Output(fmt.Sprintf("%20s", ip))
			}
		}

		this.Ui.Output(fmt.Sprintf("Total instances:%d machines:%d", len(hostPorts), len(machines)))
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

    -host
      Work with -list, print host instead of redis instance

    -add host:port

    -del host:port

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
