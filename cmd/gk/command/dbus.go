package command

import (
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/funkygao/columnize"
	"github.com/funkygao/dbus/pkg/myslave"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-helix"
	hzk "github.com/funkygao/go-helix/store/zk"
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
		zone         string
		topMode      bool
		helixCluster string
		initCluster  string
		addResource  string
		resetAddr    string
	)
	cmdFlags := flag.NewFlagSet("dbus", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&helixCluster, "c", "", "")
	cmdFlags.BoolVar(&topMode, "top", false, "")
	cmdFlags.StringVar(&initCluster, "init", "", "")
	cmdFlags.StringVar(&addResource, "add", "", "")
	cmdFlags.StringVar(&resetAddr, "reset", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		requireAdminRights("-init", "-add", "-reset").
		invalid(args) {
		return 2
	}

	if len(initCluster) != 0 {
		this.initHelixCluster(initCluster, ctx.ZoneZkAddrs(zone))
		return
	}

	if len(addResource) != 0 {
		if len(helixCluster) == 0 {
			this.Ui.Error("-c cluster is required")
			this.Ui.Output(this.Help())
			return
		}

		this.addResource(addResource, helixCluster, ctx.ZoneZkAddrs(zone))
		return
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	if len(resetAddr) != 0 {
		this.resetPosition(zkzone, resetAddr)
		return
	}

	// show dbus owner status
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

func (this *Dbus) initHelixCluster(cluster string, zkSvr string) {
	admin := hzk.NewZkHelixAdmin(zkSvr)
	swallow(admin.Connect())

	if ok, err := admin.IsClusterSetup(cluster); err != nil {
		this.Ui.Error(err.Error())
		return
	} else if ok {
		this.Ui.Warnf("cluster %s already exists, skipped")
		return
	}

	swallow(admin.AddCluster(cluster))
	swallow(admin.AllowParticipantAutoJoin(cluster, true))
	this.Ui.Infof("cluster %s created", cluster)
}

func (this *Dbus) addResource(resource, cluster, zkSvr string) {
	admin := hzk.NewZkHelixAdmin(zkSvr)
	swallow(admin.Connect())

	if ok, err := admin.IsClusterSetup(cluster); !ok || err != nil {
		this.Ui.Errorf("cluster %s not setup", cluster)
		return
	}

	partitions := 1
	resourceOption := helix.DefaultAddResourceOption(partitions, helix.StateModelOnlineOffline)
	resourceOption.RebalancerMode = helix.RebalancerModeFullAuto
	swallow(admin.AddResource(cluster, resource, resourceOption))
	swallow(admin.Rebalance(cluster, resource, 1))
	this.Ui.Infof("%s for cluster %s added and rebalanced", resource, cluster)
}

func (this *Dbus) resetPosition(zkzone *zk.ZkZone, addr string) {
	path := fmt.Sprintf("/dbus/myslave/%s", addr)
	data, _, err := zkzone.Conn().Get(path)
	swallow(err)

	var v myslave.PositionerZk
	swallow(json.Unmarshal(data, &v))
	v.Offset = 4
	data, err = json.Marshal(v)
	swallow(err)
	_, err = zkzone.Conn().Set(path, data, -1)
	swallow(err)

	this.Ui.Infof("ok for %s", path)
}

func (this *Dbus) checkMyslave(zkzone *zk.ZkZone) {
	lines := []string{"Name|Mysql|File|Offset|dbus|revision|pid|uptime|msince"}
	root := "/dbus/myslave"
	dbs, _, err := zkzone.Conn().Children(root)
	swallow(err)

	sort.Strings(dbs)

	for _, db := range dbs {
		dbRoot := fmt.Sprintf("%s/%s", root, db)
		data, _, err := zkzone.Conn().Get(dbRoot)
		var v myslave.PositionerZk
		if err = json.Unmarshal(data, &v); err != nil {
			v.File = "?"
		}

		ownerPath := fmt.Sprintf("%s/owner", dbRoot)
		owner, ownerStat, err := zkzone.Conn().Get(ownerPath)
		if err != nil {
			if err != zklib.ErrNoNode {
				panic(err)
			}

			// an orphan binlog stream: no dbus consuming it
			lines = append(lines, fmt.Sprintf("-|%s|%s|%d|-|-|-|-|-", db,
				v.File, v.Offset))
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
				lines = append(lines, fmt.Sprintf("%s|%s|%s|%d|%s|%s|%s|%s|%s",
					v.Name, db, v.File, v.Offset, instance, string(ver), pid,
					gofmt.PrettySince(uptime), gofmt.PrettySince(ownerTime)))
			} else {
				lines = append(lines, fmt.Sprintf("%s|%s| | |%s|%s|%s|%s| ",
					v.Name, db, instance, string(ver), pid, gofmt.PrettySince(uptime)))
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

    -reset mysql_host:mysql_port
      Rewind a mysql binlog position to head of the current log file.

    -init cluster
      Initialize helix cluster.

    -add mysql_host:mysql_port
      Add a mysql instance as resource to helix.
      Work with -c helix_cluster

    -top
      Run in top mode.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
