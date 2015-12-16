package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Ops struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Ops) Run(args []string) (exitCode int) {
	return
}

func (*Ops) Synopsis() string {
	return "Operations on kafka"
}

func (this *Ops) Help() string {
	help := fmt.Sprintf(`
Usage: %s [options]

    Operations on kafka

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
