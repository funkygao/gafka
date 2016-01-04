package command

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/samuel/go-zookeeper/zk"
)

type Dump struct {
	Ui  cli.Ui
	Cmd string

	zone    string
	path    string
	outfile string
	f       *os.File
}

func (this *Dump) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("dump", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.outfile, "o", "zk.dump", "")
	cmdFlags.StringVar(&this.path, "p", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-p").
		requireAdminRights("-p").
		invalid(args) {
		return 2
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	var err error
	this.f, err = os.OpenFile(this.outfile,
		os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	must(err)

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()

	this.dump(zkzone.Conn(), this.path)
	this.f.Close()

	this.Ui.Info(fmt.Sprintf("dumpped to %s", this.outfile))

	return
}

func (this *Dump) dump(conn *zk.Conn, path string) {
	children, _, err := conn.Children(path)
	if err != nil {
		must(err)
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
			// ignore ephemeral znodes
			continue
		}

		_, err = this.f.Write([]byte(znode))
		must(err)
		_, err = this.f.Write([]byte{'\n'})
		must(err)
		if len(data) > 0 {
			_, err = this.f.Write(data)
			must(err)
			_, err = this.f.Write([]byte{'\n'})
			must(err)
		}

		this.dump(conn, znode)
	}
}

func (*Dump) Synopsis() string {
	return "Dump permanent directories and contents of Zookeeper"
}

func (this *Dump) Help() string {
	help := fmt.Sprintf(`
Usage: %s dump -z zone -p path [options]

    Dump permanent directories and contents of Zookeeper

Options:

    -o outfile
      Default zk.dump

`, this.Cmd)
	return strings.TrimSpace(help)
}
