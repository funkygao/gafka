package command

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"text/template"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

// go get -u github.com/jteeuwen/go-bindata
//go:generate go-bindata -nomemcopy -pkg command template/...

type Deploy struct {
	Ui  cli.Ui
	Cmd string

	kafkaBaseDir  string
	zone, cluster string
	rootPah       string
	user          string
	brokerId      string
	tcpPort       string
	ip            string
}

func (this *Deploy) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("deploy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.kafkaBaseDir, "kafka.base", "/opt/kafka_2.10-0.8.1.1/", "")
	cmdFlags.StringVar(&this.brokerId, "broker.id", "", "")
	cmdFlags.StringVar(&this.tcpPort, "port", "", "")
	cmdFlags.StringVar(&this.rootPah, "root", "/var/wd", "")
	cmdFlags.StringVar(&this.ip, "ip", "", "")
	cmdFlags.StringVar(&this.user, "user", "sre", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-c", "-broker.id", "-port", "-ip").
		invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	clusers := zkzone.Clusters()
	zkchroot, present := clusers[this.cluster]
	if !present {
		this.Ui.Error(fmt.Sprintf("run 'gk clusters -z %s -add %s -p $zkchroot' first!",
			this.zone, this.cluster))
		return 1
	}

	if !ctx.CurrentUserIsRoot() && false {
		this.Ui.Error("requires root priviledges!")
		return 1
	}

	// prepare the root directory
	this.rootPah = strings.TrimSuffix(this.rootPah, "/")
	err := os.MkdirAll(fmt.Sprintf("%s/bin", this.instanceDir()), 0755)
	swallow(err)
	err = os.MkdirAll(fmt.Sprintf("%s/config", this.instanceDir()), 0755)
	swallow(err)
	err = os.MkdirAll(fmt.Sprintf("%s/logs", this.instanceDir()), 0755)
	swallow(err)

	type templateVar struct {
		KafkaBase   string
		BrokerId    string
		TcpPort     string
		Ip          string
		User        string
		ZkChroot    string
		ZkAddrs     string
		InstanceDir string
	}
	data := templateVar{
		ZkChroot:    zkchroot,
		KafkaBase:   this.kafkaBaseDir,
		BrokerId:    this.brokerId,
		Ip:          this.ip,
		InstanceDir: this.instanceDir(),
		User:        this.user,
		TcpPort:     this.tcpPort,
		ZkAddrs:     zkzone.ZkAddrs(),
	}

	// package the kafka runtime together
	err = os.MkdirAll(this.kafkaLibDir(), 0755)
	if err != nil {
		if !os.IsExist(err) {
			swallow(err)
		} else {
			// ok, kafka already installed
		}
	} else {
		this.installKafka()
	}

	// bin
	this.writeFileFromTemplate("template/bin/kafka-run-class.sh",
		fmt.Sprintf("%s/bin/kafka-run-class.sh", this.instanceDir()), 0755, data)
	this.writeFileFromTemplate("template/bin/kafka-server-start.sh",
		fmt.Sprintf("%s/bin/kafka-server-start.sh", this.instanceDir()), 0755, data)
	this.writeFileFromTemplate("template/bin/setenv.sh",
		fmt.Sprintf("%s/bin/setenv.sh", this.instanceDir()), 0755, data)

	// /etc/init.d/
	this.writeFileFromTemplate("template/init.d/kafka",
		//fmt.Sprintf("/etc/init.d/kfk_%s", this.cluster), 0644, data) // TODO root
		fmt.Sprintf("%s/%s", this.instanceDir(), this.clusterName()), 0755, data)

	// config
	this.writeFileFromTemplate("template/config/server.properties",
		fmt.Sprintf("%s/config/server.properties", this.instanceDir()), 0644, data)
	this.writeFileFromTemplate("template/config/log4j.properties",
		fmt.Sprintf("%s/config/log4j.properties", this.instanceDir()), 0644, data)

	this.Ui.Warn(fmt.Sprintf("deployed! REMEMBER to add monitor for this new broker!"))
	this.Ui.Warn(fmt.Sprintf("NOW, please run the following command:"))
	this.Ui.Output(color.Red("chkconfig --add %s", this.clusterName()))
	this.Ui.Output(color.Red("/etc/init.d/%s start", this.clusterName()))

	return
}

func (this *Deploy) kafkaLibDir() string {
	return fmt.Sprintf("%s/libs", this.kafkaBaseDir)
}

func (this *Deploy) instanceDir() string {
	return fmt.Sprintf("%s/%s", this.rootPah, this.clusterName())
}

func (this *Deploy) clusterName() string {
	return fmt.Sprintf("kfk_%s", this.cluster)
}

func (this *Deploy) installKafka() {

}

func (this *Deploy) writeFileFromTemplate(tplSrc, dst string, perm os.FileMode,
	data interface{}) {
	b, err := Asset(tplSrc)
	swallow(err)
	wr := &bytes.Buffer{}
	t := template.Must(template.New(tplSrc).Parse(string(b)))
	err = t.Execute(wr, data)
	swallow(err)

	// TODO chown
	err = ioutil.WriteFile(dst, wr.Bytes(), perm)
	swallow(err)
}

func (*Deploy) Synopsis() string {
	return "Deploy a new kafka broker"
}

func (this *Deploy) Help() string {
	help := fmt.Sprintf(`
Usage: %s deploy -z zone -c cluster [options]

    Deploy a new kafka broker

Options:

    -root dir
      Root directory of the kafka broker.
      Defaults to /var/wd

    -ip addr
      Advertised host name of this new broker.	

    -port
      Tcp port the broker will listen on.
      run 'gk topology -z %s -maxport' to get the max port currently in use.

    -broker.id id

    -kafka.base dir
      Kafka installation prefix dir.
      Defaults to /opt/kafka_2.10-0.8.1.1/

`, this.Cmd, this.zone)
	return strings.TrimSpace(help)
}
