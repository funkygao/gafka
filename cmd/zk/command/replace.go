package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/samuel/go-zookeeper/zk"
)

type Replace struct {
	Ui  cli.Ui
	Cmd string

	zone     string
	path     string
	from, to string
}

func (this *Replace) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("replace", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.from, "from", "", "")
	cmdFlags.StringVar(&this.to, "to", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-from", "-to").
		requireAdminRights("-f").
		invalid(args) {
		return 2
	}

	if len(args) == 0 {
		this.Ui.Error("missing path")
		return 2
	}

	this.path = args[len(args)-1]

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	doZkAuthIfNecessary(zkzone)
	defer zkzone.Close()

	this.replaceZnodes(zkzone.Conn(), this.path, this.from, this.to)

	return
}

func (this *Replace) replaceZnodes(conn *zk.Conn, path string, from, to string) {
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
			// ephemeral znode ignored
			continue
		}

		sdata := string(data)
		if len(data) > 0 && patternMatched(sdata, from) {
			replacedBy := strings.Replace(sdata, from, to, -1)
			yes, err := this.Ui.Ask(fmt.Sprintf("%s -> %s? [Y/n]", sdata, replacedBy))
			if err != nil {
				panic(err)
			}

			if yes != "n" && yes != "N" {
				_, err := conn.Set(znode, []byte(replacedBy), -1)
				if err != nil {
					panic(err)
				}
			}
		}

		this.replaceZnodes(conn, znode, from, to)
	}
}

func (*Replace) Synopsis() string {
	return "Recursively interactively replace znode values"
}

func (this *Replace) Help() string {
	help := fmt.Sprintf(`
Usage: %s replace [options] <path>

    %s

Options:

    -z zone
    
    -from from

    -to to

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
