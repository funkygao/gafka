package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Get struct {
	Ui  cli.Ui
	Cmd string

	zone      string
	path      string
	recursive bool
}

func (this *Get) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("get", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.recursive, "R", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	this.path = args[len(args)-1]

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()
	if this.recursive {
		datas := zkzone.ChildrenWithData(this.path)
		sortedPath := make([]string, 0, len(datas))
		for path, _ := range datas {
			sortedPath = append(sortedPath, path)
		}
		sort.Strings(sortedPath)

		if this.path == "/" {
			this.path = ""
		}
		for _, path := range sortedPath {
			this.Ui.Output(color.Green("%s/%s", this.path, path))
			data := datas[path]
			if len(data.Data()) > 0 {
				this.Ui.Output(fmt.Sprintf("%s %s", strings.Repeat(" ", 3),
					string(data.Data())))
			}
		}

		return 0
	}

	conn := zkzone.Conn()
	data, stat, err := conn.Get(this.path)
	must(err)

	if len(data) == 0 {
		this.Ui.Output("empty znode")
		return
	}

	this.Ui.Output(fmt.Sprintf("%#v", *stat))
	this.Ui.Output(color.Green("Data Bytes"))
	fmt.Println(data)
	this.Ui.Output(color.Green("Data as String"))
	this.Ui.Output(string(data))

	return
}

func (*Get) Synopsis() string {
	return "Show znode data"
}

func (this *Get) Help() string {
	help := fmt.Sprintf(`
Usage: %s get [options] path

    Show znode data

Options:

    -z zone

    -R
      Recursively show subdirectories znode encountered.

`, this.Cmd)
	return strings.TrimSpace(help)
}
