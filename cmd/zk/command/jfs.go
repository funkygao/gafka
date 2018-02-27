package command

import (
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/funkygao/columnize"
	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/zkclient"
	"github.com/pmylund/sortutil"
)

type tfnode struct {
	id       int
	ip       string
	port     int
	weight   int
	location int
}

func (tn tfnode) isMaster() bool {
	return tn.id%3 == 1
}

type Jfs struct {
	Ui  cli.Ui
	Cmd string

	zone string

	masterOnly  bool
	ascBy       string
	descBy      string
	groupMode   bool
	summaryMode bool

	tfnodeExp *regexp.Regexp

	zc *zkclient.Client
}

func (this *Jfs) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("jfs", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.masterOnly, "m", false, "")
	cmdFlags.StringVar(&this.ascBy, "asc", "", "")
	cmdFlags.StringVar(&this.descBy, "desc", "", "")
	cmdFlags.BoolVar(&this.summaryMode, "sum", false, "")
	cmdFlags.BoolVar(&this.groupMode, "g", false, "")

	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	doZkAuthIfNecessary(zkzone)
	defer zkzone.Close()

	this.tfnodeExp = regexp.MustCompile(`^(\d+\.\d+\.\d+\.\d+):(\d+),(\d+)\|(\d+)$`)

	var nodes []tfnode
	var hosts = make(map[string]int)
	for id, zd := range zkzone.ChildrenWithData("/jfs-root/tfnode") {
		tn := this.parseTfnode(id, zd.Data())
		if this.masterOnly && !tn.isMaster() {
			continue
		}

		nodes = append(nodes, tn)
		if _, present := hosts[tn.ip]; present {
			hosts[tn.ip]++
		} else {
			hosts[tn.ip] = 1
		}
	}

	if this.summaryMode {
		lines := []string{"Ip|Instances"}
		for ip, n := range hosts {
			lines = append(lines, fmt.Sprintf("%s|%d", ip, n))
		}
		this.Ui.Output(columnize.SimpleFormat(lines))
		return
	}

	if this.ascBy == "" && this.descBy == "" {
		sortutil.AscByField(nodes, "id")
	} else if this.ascBy != "" {
		sortutil.AscByField(nodes, this.ascBy)
	} else {
		sortutil.DescByField(nodes, this.descBy)
	}

	lines := []string{"#|Ip|Port|Weight|Location|Master"}
	for _, n := range nodes {
		lines = append(lines, fmt.Sprintf("%d|%s|%d|%d|%d|%v", n.id, n.ip, n.port, n.weight, n.location, n.isMaster()))
	}
	this.Ui.Output(columnize.SimpleFormat(lines))
	this.Ui.Output("")
	this.Ui.Outputf("Nodes:%d Hosts:%d", len(nodes), len(hosts))

	return
}

func (this *Jfs) parseTfnode(id string, d []byte) (r tfnode) {
	// d: 172.19.150.101:20130,0|1
	// FindAllStringSubmatch: [["172.19.150.101:20130,0|1" "172.19.150.101" "20130" "0" "1"]]
	tuples := this.tfnodeExp.FindAllStringSubmatch(string(d), -1)[0]
	r.ip = tuples[1]
	r.port, _ = strconv.Atoi(tuples[2])
	r.weight, _ = strconv.Atoi(tuples[3])
	r.location, _ = strconv.Atoi(tuples[4])
	r.id, _ = strconv.Atoi(id)
	return
}

func (*Jfs) Synopsis() string {
	return "List JFS related info"
}

func (this *Jfs) Help() string {
	help := fmt.Sprintf(`
Usage: %s jfs [options]

    %s

Options:

    -z zone

    -m
      Shows masters only.

    -sum
      Summary mode.

    -asc field
      Valid fields are: id|ip|port|weight|location.

    -desc field
      Valid fields are: id|ip|port|weight|location.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
