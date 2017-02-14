package command

import (
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/funkygao/columnize"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Dbus struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Dbus) Run(args []string) (exitCode int) {
	var (
		zone    string
		topMode bool
	)
	cmdFlags := flag.NewFlagSet("dbus", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&topMode, "top", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	for {
		this.checkMyslave(zkzone)
		if !topMode {
			break
		}

		time.Sleep(time.Second * 3)
		refreshScreen()
	}

	this.checkMyslave(zkzone)
	return
}

type binlogCheckpoint struct {
	File   string `json:"file"`
	Offset int    `json:"offset"`
	Owner  string `json:"owner"`
}

func (this *Dbus) checkMyslave(zkzone *zk.ZkZone) {
	lines := []string{"mysql|file|offset|dbus|role|uptime"}
	root := "/dbus/myslave"
	dbs, _, err := zkzone.Conn().Children(root)
	swallow(err)

	sort.Strings(dbs)

	for _, db := range dbs {
		dbRoot := fmt.Sprintf("%s/%s", root, db)
		data, _, err := zkzone.Conn().Get(dbRoot)
		var v binlogCheckpoint
		swallow(json.Unmarshal(data, &v))

		ownerPath := fmt.Sprintf("%s/owner", dbRoot)
		owner, _, err := zkzone.Conn().Get(ownerPath)
		swallow(err)

		idsPath := fmt.Sprintf("%s/ids", dbRoot)
		runners, _, err := zkzone.Conn().Children(idsPath)
		swallow(err)

		for _, r := range runners {
			if r == string(owner) {
				lines = append(lines, fmt.Sprintf("%s|%s|%d|%s|master|xx", db, v.File, v.Offset, r))
			} else {
				lines = append(lines, fmt.Sprintf("%s| | |%s|master|xx", db, r))
			}
		}

	}

	this.Ui.Output(columnize.SimpleFormat(lines))
}

func (*Dbus) Synopsis() string {
	return "Monitor database system runtime"
}

func (this *Dbus) Help() string {
	help := fmt.Sprintf(`
Usage: %s dbus [options]

    %s

Options:

    -z zone

    -top
      Run in top mode.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
