package command

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/pipestream"
)

type Consul struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Consul) Run(args []string) (exitCode int) {
	var zone string
	cmdFlags := flag.NewFlagSet("consul", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))

	brokerHosts := make(map[string]struct{})
	zkzone.ForSortedBrokers(func(cluster string, brokers map[string]*zk.BrokerZnode) {
		for _, brokerInfo := range brokers {
			brokerHosts[brokerInfo.Host] = struct{}{}
		}
	})

	consulLiveNode, consulDeadNodes := this.consulMembers()
	for _, node := range consulDeadNodes {
		this.Ui.Error(fmt.Sprintf("%s consul dead", node))
	}

	consulLiveMap := make(map[string]struct{})
	for _, node := range consulLiveNode {
		if _, present := brokerHosts[node]; !present {
			this.Ui.Info(fmt.Sprintf("+ %s", node))
		}

		consulLiveMap[node] = struct{}{}
	}

	for broker, _ := range brokerHosts {
		if _, present := consulLiveMap[broker]; !present {
			this.Ui.Warn(fmt.Sprintf("- %s", broker))
		}
	}

	return
}

func (this *Consul) consulMembers() ([]string, []string) {
	cmd := pipestream.New("consul", "members")
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	liveHosts, deadHosts := []string{}, []string{}
	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "Protocol") {
			// the header
			continue
		}

		fields := strings.Fields(scanner.Text())
		addr, alive := fields[1], fields[2]
		host, _, err := net.SplitHostPort(addr)
		swallow(err)

		if alive == "alive" {
			liveHosts = append(liveHosts, host)
		} else {
			deadHosts = append(deadHosts, host)
		}
	}

	return liveHosts, deadHosts
}

func (*Consul) Synopsis() string {
	return "Verify consul members match kafka zone"
}

func (this *Consul) Help() string {
	help := fmt.Sprintf(`
Usage: %s consul [options]

    Verify consul members match kafka zone

    -z zone

`, this.Cmd)
	return strings.TrimSpace(help)
}
