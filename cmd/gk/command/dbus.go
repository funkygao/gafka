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
	"github.com/funkygao/golib/gofmt"
	zklib "github.com/samuel/go-zookeeper/zk"
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

	return
}

type binlogCheckpoint struct {
	File   string `json:"file"`
	Offset int64  `json:"offset"`
	Owner  string `json:"owner"`
}

func (this *Dbus) checkMyslave(zkzone *zk.ZkZone) {
	lines := []string{"Mysql|File|Offset|dbus|ver|pid|uptime|msince"}
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
		owner, ownerStat, err := zkzone.Conn().Get(ownerPath)
		if err != nil {
			if err != zklib.ErrNoNode {
				panic(err)
			}

			// an orphan binlog stream: no dbus consuming it
			lines = append(lines, fmt.Sprintf("%s|-|-|-|-|-|-|-", db))
			continue
		}

		idsPath := fmt.Sprintf("%s/ids", dbRoot)
		runners, _, err := zkzone.Conn().Children(idsPath)
		swallow(err)

		for _, r := range runners {
			ver, stat, err := zkzone.Conn().Get(fmt.Sprintf("%s/%s", idsPath, r))
			swallow(err)

			i := strings.LastIndex(r, "-")
			instance := r[:i]
			pid := r[i+1:]
			uptime := zk.ZkTimestamp(stat.Ctime).Time()

			if r == string(owner) {
				ownerTime := zk.ZkTimestamp(ownerStat.Mtime).Time()
				lines = append(lines, fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s",
					db, v.File, gofmt.Comma(v.Offset), instance, string(ver), pid,
					gofmt.PrettySince(uptime), gofmt.PrettySince(ownerTime)))
			} else {
				lines = append(lines, fmt.Sprintf("%s| | |%s|%s|%s|%s| ",
					db, instance, string(ver), pid, gofmt.PrettySince(uptime)))
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
