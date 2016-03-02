package command

import (
	"bufio"
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/golib/pipestream"
	"github.com/ryanuber/columnize"
)

type Brokers struct {
	Ui  cli.Ui
	Cmd string

	staleOnly     bool
	maxBrokerMode bool
	showVersions  bool
	ipInNumber    bool
}

func (this *Brokers) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&this.ipInNumber, "n", false, "")
	cmdFlags.BoolVar(&this.staleOnly, "stale", false, "")
	cmdFlags.BoolVar(&this.showVersions, "versions", false, "")
	cmdFlags.BoolVar(&this.maxBrokerMode, "maxbroker", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.showVersions {
		this.doShowVersions()
		return
	}

	if zone != "" {
		ensureZoneValid(zone)

		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		if cluster != "" {
			if this.maxBrokerMode {
				maxBrokerId := this.maxBrokerId(zkzone, cluster)
				this.Ui.Output(fmt.Sprintf("%d", maxBrokerId))
				return
			}

			zkcluster := zkzone.NewCluster(cluster)
			lines, _ := this.clusterBrokers(cluster, zkcluster.Brokers())
			for _, l := range lines {
				this.Ui.Output(l)
			}

			printSwallowedErrors(this.Ui, zkzone)

			return
		}

		this.displayZoneBrokers(zkzone)
		printSwallowedErrors(this.Ui, zkzone)

		return
	}

	// print all brokers on all zones by default
	forSortedZones(func(zkzone *zk.ZkZone) {
		this.displayZoneBrokers(zkzone)

		printSwallowedErrors(this.Ui, zkzone)
	})

	return
}

func (this *Brokers) maxBrokerId(zkzone *zk.ZkZone, clusterName string) int {
	var maxBrokerId int
	zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		if cluster == clusterName {
			for _, b := range liveBrokers {
				id, _ := strconv.Atoi(b.Id)
				if id > maxBrokerId {
					maxBrokerId = id
				}
			}
		}
	})

	return maxBrokerId
}

func (this *Brokers) displayZoneBrokers(zkzone *zk.ZkZone) {
	lines := make([]string, 0)
	n := 0
	zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		if this.maxBrokerMode {
			maxBrokerId := 0
			maxPort := 0
			for _, b := range liveBrokers {
				id, _ := strconv.Atoi(b.Id)
				if id > maxBrokerId {
					maxBrokerId = id
				}
				if b.Port > maxPort {
					maxPort = b.Port
				}
			}
			lines = append(lines, fmt.Sprintf("%40s max.broker.id:%-2d max.port:%d",
				color.Blue(cluster), maxBrokerId, maxPort))
			return
		}

		outputs, count := this.clusterBrokers(cluster, liveBrokers)
		lines = append(lines, outputs...)
		n += count
	})
	this.Ui.Output(fmt.Sprintf("%s: %d", zkzone.Name(), n))
	for _, l := range lines {
		this.Ui.Output(l)
	}
}

func (this *Brokers) clusterBrokers(cluster string, brokers map[string]*zk.BrokerZnode) ([]string, int) {
	if brokers == nil || len(brokers) == 0 {
		return []string{fmt.Sprintf("%s%s %s", strings.Repeat(" ", 4),
			cluster, color.Red("empty brokers"))}, 0
	}

	lines := make([]string, 0, len(brokers))
	lines = append(lines, strings.Repeat(" ", 4)+cluster)
	if this.staleOnly {
		// try each broker's aliveness
		n := 0
		for brokerId, broker := range brokers {
			kfk, err := sarama.NewClient([]string{broker.Addr()}, sarama.NewConfig())
			if err != nil {
				n++
				lines = append(lines, color.Yellow("%8s %s %s",
					brokerId, broker, err.Error()))
			} else {
				kfk.Close()
			}
		}

		return lines, n
	}

	// sort by broker id
	sortedBrokerIds := make([]string, 0, len(brokers))
	for brokerId, _ := range brokers {
		sortedBrokerIds = append(sortedBrokerIds, brokerId)
	}
	sort.Strings(sortedBrokerIds)

	for _, brokerId := range sortedBrokerIds {
		b := brokers[brokerId]
		uptime := gofmt.PrettySince(b.Uptime())
		if time.Since(b.Uptime()) < time.Hour*24*7 {
			uptime = color.Green(uptime)
		}
		if this.ipInNumber {
			lines = append(lines, fmt.Sprintf("\t%8s %21s jmx:%-2d ver:%-2d uptime:%s",
				brokerId,
				b.Addr(),
				b.JmxPort,
				b.Version, uptime))
		} else {
			lines = append(lines, fmt.Sprintf("\t%8s %21s jmx:%-2d ver:%-2d uptime:%s",
				brokerId,
				b.NamedAddr(),
				b.JmxPort,
				b.Version, uptime))
		}

	}
	return lines, len(brokers)
}

func (this *Brokers) doShowVersions() {
	kafkaVerExp := regexp.MustCompile(`/kafka_(?P<ver>[-\d.]*)\.jar`)
	processExp := regexp.MustCompile(`kfk_(?P<process>\S*)/config/server.properties`)

	cmd := pipestream.New("/usr/bin/consul", "exec",
		"pgrep", "-lf", "java",
		"|", "grep", "-w", "kafka",
		"|", "grep", "-vw", "grep")
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)

	var (
		line     string
		lastLine string
	)
	hosts := make(map[string]struct{})
	lines := make([]string, 0)
	header := "Host|Process|Version"
	lines = append(lines, header)
	for scanner.Scan() {
		line = scanner.Text()
		if strings.Contains(line, "finished with exit code") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		if _, err := strconv.Atoi(fields[1]); err != nil {
			// field 1 should be pid
			// if not pid, it continues with last line
			line = lastLine + strings.Join(fields[1:], " ")

			// redo fields
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
		}

		lastLine = line

		// version
		matched := kafkaVerExp.FindStringSubmatch(line)
		if len(matched) < 2 {
			continue
		}
		ver := matched[1]

		// process name
		matched = processExp.FindStringSubmatch(line)
		if len(matched) < 2 {
			continue
		}
		process := matched[1]

		// got a valid process record
		host := fields[0][0 : len(fields[0])-1] // discard the ending ':'
		hosts[host] = struct{}{}
		lines = append(lines, fmt.Sprintf("%s|%s|%s", host, process, ver))
	}
	swallow(scanner.Err())

	this.Ui.Output(columnize.SimpleFormat(lines))
	this.Ui.Output("")
	this.Ui.Output(fmt.Sprintf("TOTAL %d processes running on %d hosts",
		len(lines)-1, len(hosts)))
}

func (*Brokers) Synopsis() string {
	return "Print online brokers from Zookeeper"
}

func (this *Brokers) Help() string {
	help := fmt.Sprintf(`
Usage: %s brokers [options]

    Print online brokers from Zookeeper

Options:

    -z zone
      Only print brokers within a zone

    -c cluster name
      Only print brokers of this cluster

    -versions
      Display kafka instances versions by host

    -maxbroker
      Display max broker.id and max port

    -n
      Show network addresses as numbers

    -stale
      Only print stale brokers: found in zk but not connectable

`, this.Cmd)
	return strings.TrimSpace(help)
}
