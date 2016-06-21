package command

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
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

	staleOnly    bool
	showVersions bool
	ipInNumber   bool
	cluster      string
}

func (this *Brokers) Run(args []string) (exitCode int) {
	var (
		zone  string
		debug bool
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.BoolVar(&debug, "debug", false, "")
	cmdFlags.BoolVar(&this.ipInNumber, "n", false, "")
	cmdFlags.BoolVar(&this.staleOnly, "stale", false, "")
	cmdFlags.BoolVar(&this.showVersions, "versions", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.showVersions {
		this.doShowVersions()
		return
	}

	if debug {
		sarama.Logger = log.New(os.Stderr, color.Magenta("[sarama]"), log.LstdFlags)
	}

	if zone != "" {
		ensureZoneValid(zone)

		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		this.displayZoneBrokers(zkzone)

		return
	}

	// print all brokers on all zones by default
	forSortedZones(func(zkzone *zk.ZkZone) {
		this.displayZoneBrokers(zkzone)
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
	header := "Zone|Cluster|Id|Broker|Uptime"
	lines = append(lines, header)

	n := 0
	zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		outputs := this.clusterBrokers(zkzone.Name(), cluster, liveBrokers)
		n += len(outputs)
		lines = append(lines, outputs...)
	})
	if this.staleOnly {
		this.Ui.Info(fmt.Sprintf("%d problematic brokers in zone[%s]", n, zkzone.Name()))
	} else {
		this.Ui.Info(fmt.Sprintf("%d brokers in zone[%s]", n, zkzone.Name()))
	}
	if len(lines) > 1 {
		// lines has header
		this.Ui.Output(columnize.SimpleFormat(lines))
	}
}

func (this *Brokers) clusterBrokers(zone, cluster string, brokers map[string]*zk.BrokerZnode) []string {
	if !patternMatched(cluster, this.cluster) {
		return nil
	}

	if brokers == nil || len(brokers) == 0 {
		return []string{fmt.Sprintf("%s|%s|%s|%s|%s",
			zone, cluster, " ", color.Red("empty brokers"), " ")}
	}

	lines := make([]string, 0, len(brokers))
	if this.staleOnly {
		// try each broker's aliveness
		for brokerId, broker := range brokers {
			cf := sarama.NewConfig()
			cf.Net.ReadTimeout = time.Second * 4
			cf.Net.WriteTimeout = time.Second * 4
			kfk, err := sarama.NewClient([]string{broker.Addr()}, cf)
			if err != nil {
				lines = append(lines, fmt.Sprintf("%s|%s|%s|%s|%s",
					zone, cluster,
					brokerId, broker.Addr(),
					fmt.Sprintf("%s: %v", gofmt.PrettySince(broker.Uptime()), err)))
			} else {
				kfk.Close()
			}
		}

		return lines
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
			lines = append(lines, fmt.Sprintf("%s|%s|%s|%s|%s",
				zone, cluster,
				brokerId, b.Addr(),
				gofmt.PrettySince(b.Uptime())))
		} else {
			lines = append(lines, fmt.Sprintf("%s|%s|%s|%s|%s",
				zone, cluster,
				brokerId, b.NamedAddr(),
				gofmt.PrettySince(b.Uptime())))
		}

	}
	return lines
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
	header := "Process|Host|Version"
	lines = append(lines, header)
	records := make(map[string]map[string]string) // {process: {host: ver}}
	for scanner.Scan() {
		line = scanner.Text()
		if strings.Contains(line, "finished with exit code") {
			continue
		}
		if strings.Contains(line, "node(s) completed") {
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
		if _, present := records[process]; !present {
			records[process] = make(map[string]string)
		}
		host := fields[0][0 : len(fields[0])-1] // discard the ending ':'
		hosts[host] = struct{}{}
		records[process][host] = ver
	}
	swallow(scanner.Err())

	sortedProceses := make([]string, 0, len(records))
	for proc, _ := range records {
		sortedProceses = append(sortedProceses, proc)
	}
	sort.Strings(sortedProceses)

	procsWithSingleInstance := make([]string, 0)
	for _, proc := range sortedProceses {
		sortedHosts := make([]string, 0, len(records[proc]))
		for host, _ := range records[proc] {
			sortedHosts = append(sortedHosts, host)
		}
		sort.Strings(sortedHosts)

		if len(sortedHosts) < 2 {
			procsWithSingleInstance = append(procsWithSingleInstance, proc)
		}

		for _, host := range sortedHosts {
			lines = append(lines, fmt.Sprintf("%s|%s|%s", proc, host, records[proc][host]))
		}
	}

	this.Ui.Output(columnize.SimpleFormat(lines))
	this.Ui.Output("")
	this.Ui.Output(fmt.Sprintf("TOTAL %d processes running on %d hosts",
		len(lines)-1, len(hosts)))
	if len(procsWithSingleInstance) > 0 {
		this.Ui.Output(fmt.Sprintf("\nProcess with 1 SPOF: "))
		this.Ui.Warn(fmt.Sprintf("%v", procsWithSingleInstance))
	}
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
      Precondition: you MUST install consul on each broker host

    -debug

    -n
      Show network addresses as numbers

    -stale
      Only print stale brokers: found in zk but not connectable

`, this.Cmd)
	return strings.TrimSpace(help)
}
