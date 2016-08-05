package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/ryanuber/columnize"
)

type Job struct {
	Ui  cli.Ui
	Cmd string

	cluster string
}

func (this *Job) Run(args []string) (exitCode int) {
	var (
		zone string
	)
	cmdFlags := flag.NewFlagSet("job", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "") // TODO not used
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	this.printJob(zkzone)

	return
}

func (this *Job) printJob(zkzone *zk.ZkZone) {
	lines := make([]string, 0)
	header := "JobId|Cluster|Ctime|Mtime"
	lines = append(lines, header)
	jobs := zkzone.ChildrenWithData(zk.PubsubJobQueues)
	sortedName := make([]string, 0, len(jobs))
	for name := range jobs {
		sortedName = append(sortedName, name)
	}
	sort.Strings(sortedName)

	for _, name := range sortedName {
		zdata := jobs[name]
		lines = append(lines, fmt.Sprintf("%s|%s|%s|%s", name,
			string(zdata.Data()), zdata.Ctime(), zdata.Mtime()))
	}
	if len(lines) > 1 {
		this.Ui.Output(columnize.SimpleFormat(lines))
	}

	lines = lines[:0]
	header = "Actor|Ctime|Mtime"
	actors := zkzone.ChildrenWithData(zk.PubsubActors)
	sortedName = make([]string, 0, len(actors))
	for name := range actors {
		sortedName = append(sortedName, name)
	}
	sort.Strings(sortedName)

	for _, name := range sortedName {
		zdata := actors[name]
		lines = append(lines, fmt.Sprintf("%s|%s|%s", name,
			zdata.Ctime(), zdata.Mtime()))
	}
	if len(lines) > 1 {
		this.Ui.Output(columnize.SimpleFormat(lines))
	}
}

func (*Job) Synopsis() string {
	return "Display job/actor related znodes for PubSub system."
}

func (this *Job) Help() string {
	help := fmt.Sprintf(`
Usage: %s job [options]

    %s

Options:

    -z zone

    -c cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
