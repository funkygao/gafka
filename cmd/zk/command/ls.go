package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/zkclient"
	"github.com/samuel/go-zookeeper/zk"
)

type Ls struct {
	Ui  cli.Ui
	Cmd string

	zone        string
	path        string
	recursive   bool
	watch       bool
	likePattern string

	zc *zkclient.Client
}

func (this *Ls) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("ls", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.recursive, "R", false, "")
	cmdFlags.BoolVar(&this.watch, "w", false, "")
	cmdFlags.StringVar(&this.likePattern, "like", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing path")
		return 2
	}

	this.path = args[len(args)-1]

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()
	conn := zkzone.Conn()

	if this.recursive && !this.watch {
		this.showChildrenRecursively(conn, this.path)
		return
	}

	if this.watch {
		this.watchChildren(ctx.ZoneZkAddrs(this.zone))
		return
	}

	children, _, err := conn.Children(this.path)
	must(err)
	sort.Strings(children)
	if this.path == "/" {
		this.path = ""
	}
	for _, child := range children {
		this.Ui.Output(this.path + "/" + child)
	}

	return
}

func (this *Ls) watchChildren(zkConnStr string) {
	zc := zkclient.New(zkConnStr)
	must(zc.Connect())

	children, err := zc.Children(this.path)
	must(err)
	sort.Strings(children)
	this.Ui.Outputf("%+v", children)

	this.zc = zc

	zc.SubscribeChildChanges(this.path, this)
	select {}

}

func (this *Ls) HandleChildChange(parentPath string, lastChilds []string) error {
	children, err := this.zc.Children(this.path)
	sort.Strings(children)
	must(err)
	this.Ui.Outputf("%s %+v -> %+v", time.Now(), lastChilds, children)
	return nil
}

func (this *Ls) showChildrenRecursively(conn *zk.Conn, path string) {
	children, _, err := conn.Children(path)
	if err != nil {
		return
	}

	sort.Strings(children)
	for _, child := range children {
		if path == "/" {
			path = ""
		}

		znode := path + "/" + child

		if patternMatched(znode, this.likePattern) {
			this.Ui.Output(znode)
		}

		this.showChildrenRecursively(conn, znode)
	}
}

func (*Ls) Synopsis() string {
	return "List znode children"
}

func (this *Ls) Help() string {
	help := fmt.Sprintf(`
Usage: %s ls [options] <path>

    List znode children

Options:

    -z zone

    -R
      Recursively list subdirectories encountered.

    -w
      Keep watching the children znode changes.

    -like pattern
      Only display znode whose path is like this pattern.

`, this.Cmd)
	return strings.TrimSpace(help)
}
