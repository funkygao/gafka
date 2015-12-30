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
	"github.com/samuel/go-zookeeper/zk"
)

type Get struct {
	Ui  cli.Ui
	Cmd string

	zone      string
	path      string
	verbose   bool
	recursive bool
}

func (this *Get) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("get", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.verbose, "l", false, "")
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
		this.showChildrenRecursively(zkzone.Conn(), this.path)

		return 0
	}

	conn := zkzone.Conn()
	data, stat, err := conn.Get(this.path)
	must(err)

	if len(data) == 0 {
		this.Ui.Output("empty znode")
		return
	}

	if this.verbose {
		this.Ui.Output(color.Magenta("%#v", *stat))
	}

	this.Ui.Output(color.Green("Data Bytes"))
	fmt.Println(data)
	this.Ui.Output(color.Green("Data as String"))
	this.Ui.Output(string(data))

	return
}

func (this *Get) showChildrenRecursively(conn *zk.Conn, path string) {
	children, _, err := conn.Children(path)
	if err != nil {
		return
	}

	sort.Strings(children)
	for _, child := range children {
		if path == "/" {
			path = ""
		}

		znode := fmt.Sprintf("%s/%s", path, child)

		// display znode content
		data, stat, err := conn.Get(znode)
		must(err)
		if stat.EphemeralOwner > 0 {
			this.Ui.Output(color.Yellow(znode))
		} else {
			this.Ui.Output(color.Green(znode))
		}

		if len(data) > 0 {
			if this.verbose {
				this.Ui.Output(fmt.Sprintf("%s %#v",
					strings.Repeat(" ", 3), stat))
				this.Ui.Output(fmt.Sprintf("%s %v",
					strings.Repeat(" ", 3), data))
			}
			this.Ui.Output(fmt.Sprintf("%s %s",
				strings.Repeat(" ", 3), string(data)))
		}

		this.showChildrenRecursively(conn, znode)
	}
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

    -l
      Use a long display format.

`, this.Cmd)
	return strings.TrimSpace(help)
}
