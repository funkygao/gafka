package command

import (
	"flag"
	"fmt"
	"os/user"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

// go get -u github.com/jteeuwen/go-bindata
//go:generate go-bindata -nomemcopy template/...

type Deploy struct {
	Ui  cli.Ui
	Cmd string

	zone, cluster string
}

func (this *Deploy) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("deploy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).require("-z", "-c").invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	clusers := zkzone.Clusters()
	if _, present := clusers[this.cluster]; !present {
		this.Ui.Error(fmt.Sprintf("run 'gk clusters -z %s -add %s -p $zkpath' first!",
			this.zone, this.cluster))
		return 1
	}

	if !ctx.CurrentUserIsRoot() {
		this.Ui.Error("requires root priviledges!")
		return 1
	}

	return
}

func (*Deploy) Synopsis() string {
	return "Deploy a new kafka broker"
}

func (this *Deploy) Help() string {
	help := fmt.Sprintf(`
Usage: %s deploy [options]

    Deploy a new kafka broker

How to add a cluster?
1. 
    bin/kafka-run-class.sh bin/kafka-server-start.sh
2. 
    /etc/init.d/kfk_$cluster 
    chkconfig --add kfk_$cluster
3.
    rm -rf log/*
    vi config/server.properties
        broker.id
        port
        log.dirs
        zookeeper.connect
        auto.create.topics.enable=false
        default.replication.factor=2
4.
    notify zabbix monitor proc num        

`, this.Cmd)
	return strings.TrimSpace(help)
}
