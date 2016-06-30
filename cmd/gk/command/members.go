package command

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/pipestream"
	"github.com/ryanuber/columnize"
)

// consul members will include:
// - zk cluster as server
// - agents
//   - brokers
//   - kateway
type Members struct {
	Ui  cli.Ui
	Cmd string

	brokerHosts, zkHosts, katewayHosts map[string]struct{}
	nodeHostMap                        map[string]string // consul members node->ip
}

func (this *Members) Run(args []string) (exitCode int) {
	var (
		zone        string
		showLoadAvg bool
		exec        string
	)
	cmdFlags := flag.NewFlagSet("members", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&showLoadAvg, "l", false, "")
	cmdFlags.StringVar(&exec, "exec", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	this.fillTheHosts(zkzone)

	consulLiveNode, consulDeadNodes := this.consulMembers()
	for _, node := range consulDeadNodes {
		this.Ui.Error(fmt.Sprintf("%s consul dead", node))
	}

	consulLiveMap := make(map[string]struct{})
	brokerN, zkN, katewayN, unknownN := 0, 0, 0, 0
	for _, node := range consulLiveNode {
		_, presentInBroker := this.brokerHosts[node]
		_, presentInZk := this.zkHosts[node]
		_, presentInKateway := this.katewayHosts[node]
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
		}

		consulLiveMap[node] = struct{}{}
	}

	// all brokers should run consul
	for broker, _ := range this.brokerHosts {
		if _, present := consulLiveMap[broker]; !present {
			this.Ui.Warn(fmt.Sprintf("- %s", broker))
		}
	}

	switch {
	case showLoadAvg:
		this.displayLoadAvg()

	case exec != "":
		this.executeOnAll(exec)
	}

	// summary
	this.Ui.Output(fmt.Sprintf("zk:%s broker:%s kateway:%s ?:%s",
		color.Magenta("%d", zkN),
		color.Magenta("%d", brokerN),
		color.Magenta("%d", katewayN),
		color.Green("%d", unknownN)))

	return
}

func (this *Members) fillTheHosts(zkzone *zk.ZkZone) {
	this.brokerHosts = make(map[string]struct{})
	zkzone.ForSortedBrokers(func(cluster string, brokers map[string]*zk.BrokerZnode) {
		for _, brokerInfo := range brokers {
			this.brokerHosts[brokerInfo.Host] = struct{}{}
		}
	})

	this.zkHosts = make(map[string]struct{})
	for _, addr := range zkzone.ZkAddrList() {
		zkNode, _, err := net.SplitHostPort(addr)
		swallow(err)
		this.zkHosts[zkNode] = struct{}{}
	}

	this.katewayHosts = make(map[string]struct{})
	kws, err := zkzone.KatewayInfos()
	swallow(err)
	for _, kw := range kws {
		host, _, err := net.SplitHostPort(kw.PubAddr)
		swallow(err)
		this.katewayHosts[host] = struct{}{}
	}
}

func (this *Members) executeOnAll(execCmd string) {
	args := []string{"exec"}
	args = append(args, strings.Split(execCmd, " ")...)
	cmd := pipestream.New("consul", args...)
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	this.Ui.Info(fmt.Sprintf("%s ...", execCmd))

	lines := make([]string, 0)
	header := "Node|Host|Role|Result"
	lines = append(lines, header)

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "finished with exit code 0") ||
			strings.Contains(line, "completed / acknowledged") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 1 {
			continue
		}

		node := fields[0]
		if strings.HasSuffix(node, ":") {
			node = strings.TrimRight(node, ":")
		}

		host := this.nodeHostMap[node]
		lines = append(lines, fmt.Sprintf("%s|%s|%s|%s", node, host, this.roleOfHost(host),
			strings.Join(fields[1:], " ")))
	}

	if len(lines) > 1 {
		sort.Strings(lines[1:])
		this.Ui.Output(columnize.SimpleFormat(lines))
	}
}

func (this *Members) displayLoadAvg() {
	cmd := pipestream.New("consul", "exec",
		"uptime", "|", "grep", "load")
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	lines := make([]string, 0)
	header := "Node|Host|Role|Load Avg"
	lines = append(lines, header)

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		node := fields[0]
		parts := strings.Split(line, "load average:")
		if len(parts) < 2 {
			continue
		}
		if strings.HasSuffix(node, ":") {
			node = strings.TrimRight(node, ":")
		}

		host := this.nodeHostMap[node]
		lines = append(lines, fmt.Sprintf("%s|%s|%s|%s", node, host, this.roleOfHost(host), parts[1]))
	}

	if len(lines) > 1 {
		sort.Strings(lines[1:])
		this.Ui.Output(columnize.SimpleFormat(lines))
	}
}

func (this *Members) roleOfHost(host string) string {
	if _, present := this.brokerHosts[host]; present {
		return "B"
	}
	if _, present := this.zkHosts[host]; present {
		return "Z"
	}
	if _, present := this.katewayHosts[host]; present {
		return "K"
	}
	return "?"
}

func (this *Members) consulMembers() ([]string, []string) {
	cmd := pipestream.New("consul", "members")
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	liveHosts, deadHosts := []string{}, []string{}
	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	this.nodeHostMap = make(map[string]string)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "Protocol") {
			// the header
			continue
		}

		fields := strings.Fields(scanner.Text())
		node, addr, alive := fields[0], fields[1], fields[2]
		host, _, err := net.SplitHostPort(addr)
		swallow(err)

		this.nodeHostMap[node] = host

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

    -exec <cmd>
      Execute cmd on all members and print the result by host
      e,g. gk members -exec "ifconfig bond0 | grep 'TX bytes'"

`, this.Cmd)
	return strings.TrimSpace(help)
}
