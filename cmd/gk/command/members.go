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
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/pipestream"
)

// consul members will include:
// - zk cluster as server
// - agents
//   - brokers
//   - kateway
type Members struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Members) Run(args []string) (exitCode int) {
	var (
		zone        string
		showLoadAvg bool
	)
	cmdFlags := flag.NewFlagSet("members", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&showLoadAvg, "l", false, "")
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

	zkHosts := make(map[string]struct{})
	for _, addr := range zkzone.ZkAddrList() {
		zkNode, _, err := net.SplitHostPort(addr)
		swallow(err)
		zkHosts[zkNode] = struct{}{}
	}

	katewayHosts := make(map[string]struct{})
	kws, err := zkzone.KatewayInfos()
	swallow(err)
	for _, kw := range kws {
		host, _, err := net.SplitHostPort(kw.PubAddr)
		swallow(err)
		katewayHosts[host] = struct{}{}
	}

	consulLiveNode, consulDeadNodes := this.consulMembers()
	for _, node := range consulDeadNodes {
		this.Ui.Error(fmt.Sprintf("%s consul dead", node))
	}

	consulLiveMap := make(map[string]struct{})
	brokerN, zkN, katewayN, unknownN := 0, 0, 0, 0
	for _, node := range consulLiveNode {
		_, presentInBroker := brokerHosts[node]
		_, presentInZk := zkHosts[node]
		_, presentInKateway := katewayHosts[node]
		if presentInBroker {
			brokerN++
		}
		if presentInZk {
			zkN++
		}
		if presentInKateway {
			katewayN++
		}

		if !presentInBroker && !presentInZk && !presentInKateway {
			unknownN++

			this.Ui.Info(fmt.Sprintf("? %s", node))
		}

		consulLiveMap[node] = struct{}{}
	}

	// all brokers should run consul
	for broker, _ := range brokerHosts {
		if _, present := consulLiveMap[broker]; !present {
			this.Ui.Warn(fmt.Sprintf("- %s", broker))
		}
	}

	if showLoadAvg {
		this.displayLoadAvg()
	}

	this.Ui.Output(fmt.Sprintf("zk:%d broker:%s kateway:%s ?:%d",
		color.Magenta("%d", zkN),
		color.Magenta("%d", brokerN),
		color.Magenta("%d", katewayN),
		color.Green("%d", unknownN)))

	return
}

func (this *Members) displayLoadAvg() {
	cmd := pipestream.New("consul", "exec",
		"uptime", "|", "grep", "load")
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		host := fields[0]
		parts := strings.Split(line, "load average:")
		if len(parts) < 2 {
			continue
		}

		this.Ui.Output(fmt.Sprintf("%35s %s", host, parts[1]))
	}
}

func (this *Members) consulMembers() ([]string, []string) {
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

func (*Members) Synopsis() string {
	return "Verify consul members match kafka zone"
}

func (this *Members) Help() string {
	help := fmt.Sprintf(`
Usage: %s members [options]

    Verify consul members match kafka zone

    -z zone

    -l
      Display each member load average

`, this.Cmd)
	return strings.TrimSpace(help)
}
