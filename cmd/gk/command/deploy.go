package command

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os/user"
	"strings"
	"text/template"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

// go get -u github.com/jteeuwen/go-bindata
//go:generate go-bindata -nomemcopy -pkg command template/...

type Deploy struct {
	Ui  cli.Ui
	Cmd string

	zone, cluster string
	rootPah       string
	user          string
	brokerId      string
	tcpPort       string
}

func (this *Deploy) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("deploy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.brokerId, "broker.id", "", "")
	cmdFlags.StringVar(&this.tcpPort, "port", "", "")
	cmdFlags.StringVar(&this.rootPah, "root", "/var/wd", "")
	cmdFlags.StringVar(&this.user, "user", "sre", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-c", "-broker.id", "-port").
		invalid(args) {
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

	data := make(map[string]string)
	this.writeFileFromTemplate("template/bin/kafka-run-class.sh",
		fmt.Sprintf("%s/bin/kafka-run-class.sh", this.rootPah), 0644, data)

	data = make(map[string]string)
	this.writeFileFromTemplate("template/bin/kafka-server-start.sh",
		fmt.Sprintf("%s/bin/kafka-run-class.sh", this.rootPah), 0644, data)

	data = make(map[string]string)
	this.writeFileFromTemplate("template/config/server.properties",
		fmt.Sprintf("%s/config/server.properties", this.rootPah), 0644, data)

	data = make(map[string]string)
	this.writeFileFromTemplate("template/init.d/kafka",
		fmt.Sprintf("/etc/init.d/kfk_%s", this.cluster), 0644, data) // TODO root

	return
}

func (this *Deploy) writeFileFromTemplate(tplSrc, dst string, perm os.FileMode, data map[string]string) {
	b, err := Asset(tplSrc)
	swallow(err)
	wr := &bytes.Buffer{}
	t := template.Must(template.New(tplSrc).Parse(string(b)))
	err = t.Execute(wr, data)
	swallow(err)

	// TODO chown
	err = ioutil.WriteFile(fmt.Sprintf("%s/bin/kafka-run-class.sh", this.rootPah),
		wr.Bytes(), perm)
	swallow(err)
}

func (*Deploy) Synopsis() string {
	return "Deploy a new kafka broker"
}

func (this *Deploy) Help() string {
	help := fmt.Sprintf(`
Usage: %s deploy [options]

    Deploy a new kafka broker

Options:

    -root
      Root directory of the kafka broker.
      Defaults to /var/wd

    -port
      Tcp port the broker will listen on.
      run 'gk topology -z %s -maxport' to get the max port currently in use.

    -broker.id

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

5. start the server

`, this.Cmd, this.zone)
	return strings.TrimSpace(help)
}
