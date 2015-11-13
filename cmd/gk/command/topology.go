package command

import (
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Topology struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Topology) Run(args []string) (exitCode int) {
	if len(args) > 0 {
		for _, zone := range args {
			ensureZoneValid(zone)

			zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
			this.displayZoneTopology(zone, zkzone)
		}

		return
	}

	// print all brokers on all zones by default
	forAllZones(func(zone string, zkzone *zk.ZkZone) {
		this.displayZoneTopology(zone, zkzone)
	})

	return

}

func (this *Topology) displayZoneTopology(zone string, zkzone *zk.ZkZone) {
	this.Ui.Output(zone)

	instances := make(map[string][]int)
	zkzone.WithinBrokers(func(cluster string, brokers map[string]*zk.Broker) {
		for _, broker := range brokers {
			if _, present := instances[broker.Host]; !present {
				instances[broker.Host] = make([]int, 0)
			}
			instances[broker.Host] = append(instances[broker.Host], broker.Port)
		}
	})

	// sort by host ip
	sortedHosts := make([]string, 0, len(instances))
	for host, _ := range instances {
		sortedHosts = append(sortedHosts, host)
	}
	sort.Strings(sortedHosts)

	for _, host := range sortedHosts {
		this.Ui.Output(fmt.Sprintf("\t%s %+v", color.Green(host),
			instances[host]))
	}

}

func (*Topology) Synopsis() string {
	return "Print server topology of kafka clusters"
}

func (this *Topology) Help() string {
	help := fmt.Sprintf(`
Usage: %s topology [zone ...]

	Print server topology of kafka clusters
`, this.Cmd)
	return strings.TrimSpace(help)
}
