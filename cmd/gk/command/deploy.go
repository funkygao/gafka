package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	gio "github.com/funkygao/golib/io"
	"github.com/pmezard/go-difflib/difflib"
)

type templateVar struct {
	KafkaBase             string
	BrokerId              string
	TcpPort               string
	Ip                    string
	User                  string
	ZkChroot              string
	ZkAddrs               string
	InstanceDir           string
	LogDirs               string
	IoThreads             string
	NetworkThreads        string
	InfluxReporterEnabled string
	InfluxDbHost          string
	InfluxDbPort          string
}

// go get -u github.com/jteeuwen/go-bindata
//go:generate go-bindata -nomemcopy -pkg command template/...

type Deploy struct {
	Ui  cli.Ui
	Cmd string

	zkzone           *zk.ZkZone
	kafkaBaseDir     string
	zone, cluster    string
	rootPah          string
	runAs            string
	userInfo         *user.User
	brokerId         string
	tcpPort          string
	ip               string
	demoMode         bool
	uninstall        string
	kafkaVer         string
	logDirs          string
	influxDbAddr     string
	influxDbHost     string
	influxdbPort     string
	dryRun           bool
	installKafkaOnly bool
	installHelixOnly bool
	diffMode         bool
}

// TODO
// 1. broker id assignment
// 2. port assignment
func (this *Deploy) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("deploy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.kafkaBaseDir, "kafka.base", ctx.KafkaHome(), "")
	cmdFlags.StringVar(&this.brokerId, "broker.id", "", "")
	cmdFlags.StringVar(&this.tcpPort, "port", "", "")
	cmdFlags.StringVar(&this.rootPah, "root", "/var/wd", "")
	cmdFlags.StringVar(&this.ip, "ip", "", "")
	cmdFlags.StringVar(&this.logDirs, "log.dirs", "", "")
	cmdFlags.StringVar(&this.runAs, "user", "sre", "")
	cmdFlags.BoolVar(&this.installHelixOnly, "helix", false, "")
	cmdFlags.StringVar(&this.uninstall, "uninstall", "", "")
	cmdFlags.BoolVar(&this.diffMode, "diff", false, "")
	cmdFlags.BoolVar(&this.demoMode, "demo", false, "")
	cmdFlags.BoolVar(&this.installKafkaOnly, "kfkonly", false, "")
	cmdFlags.BoolVar(&this.dryRun, "dryrun", true, "")
	cmdFlags.StringVar(&this.influxDbAddr, "influx", "", "")
	cmdFlags.StringVar(&this.kafkaVer, "ver", "2.10-0.8.2.2", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-c").
		invalid(args) {
		return 2
	}

	if this.uninstall != "" {
		serverProperties := fmt.Sprintf("%s/config/server.properties", this.uninstall)
		lines, err := gio.ReadLines(serverProperties)
		if err != nil {
			this.Ui.Error(err.Error())
			return 2
		}
		var logDirs []string
		for _, line := range lines {
			if strings.HasPrefix(line, "log.dirs") {
				parts := strings.SplitN(line, "=", 2)
				logDirs = strings.Split(parts[1], ",")
				break
			}
		}
		if len(logDirs) == 0 {
			this.Ui.Error("empty log.dirs")
			return 2
		}

		for _, logDir := range logDirs {
			this.Ui.Output(fmt.Sprintf("rm -rf %s", logDir))
		}
		name := filepath.Base(this.uninstall)
		this.Ui.Output(fmt.Sprintf("chkconfig --del %s", name))
		this.Ui.Output(fmt.Sprintf("rm -f /etc/init.d/%s", name))
		this.Ui.Output(fmt.Sprintf("rm -rf %s", this.uninstall))
		return 0
	}

	this.zkzone = zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	clusers := this.zkzone.Clusters()
	zkchroot, present := clusers[this.cluster]
	if !present {
		this.Ui.Error(fmt.Sprintf("run 'gk clusters -z %s -add %s -p $zkchroot' first!",
			this.zone, this.cluster))
		return 1
	}

	if this.demoMode {
		this.demo()
		return
	}

	if this.diffMode {
		from := fmt.Sprintf("%s/config/server.properties", this.instanceDir())
		to := "/tmp/svr.prop"
		influxReporterEnabled := "false"
		if this.influxDbHost != "" {
			influxReporterEnabled = "true"
		}
		data := templateVar{
			ZkAddrs:               this.zkzone.ZkAddrs(),
			ZkChroot:              zkchroot,
			KafkaBase:             this.kafkaBaseDir,
			BrokerId:              this.brokerId,
			Ip:                    this.ip,
			InstanceDir:           this.instanceDir(),
			User:                  this.runAs,
			TcpPort:               this.tcpPort,
			LogDirs:               this.logDirs,
			InfluxReporterEnabled: influxReporterEnabled,
			InfluxDbHost:          this.influxDbHost,
			InfluxDbPort:          this.influxdbPort,
		}
		data.IoThreads = strconv.Itoa(3 * len(strings.Split(data.LogDirs, ",")))
		networkThreads := ctx.NumCPU() / 2
		if networkThreads < 2 {
			networkThreads = 2
		}
		data.NetworkThreads = strconv.Itoa(networkThreads) // TODO not used yet
		writeFileFromTemplate("template/config/server.properties",
			to, 0644, data, nil)

		s1, err := ioutil.ReadFile(from)
		swallow(err)
		s2, err := ioutil.ReadFile(to)
		swallow(err)

		diff := difflib.UnifiedDiff{
			A:        difflib.SplitLines(string(s1)),
			B:        difflib.SplitLines(string(s2)),
			FromFile: "Original",
			ToFile:   "Current",
			Context:  1,
		}
		text, _ := difflib.GetUnifiedDiffString(diff)
		this.Ui.Output(text)
		return
	}

	if !ctx.CurrentUserIsRoot() {
		this.Ui.Error("requires root privileges!")
		return 1
	}

	if !strings.HasSuffix(this.kafkaBaseDir, this.kafkaVer) {
		this.Ui.Error(fmt.Sprintf("kafka.base[%s] does not match ver[%s]",
			this.kafkaBaseDir, this.kafkaVer))
		return 1
	}

	if this.installKafkaOnly {
		this.installKafka()
		return
	}

	if this.installHelixOnly {
		this.installHelix()
		return
	}

	var err error
	this.userInfo, err = user.Lookup(this.runAs)
	swallow(err)

	if validateArgs(this, this.Ui).
		require("-broker.id", "-port", "-ip", "-log.dirs").
		invalid(args) {
		return 2
	}

	if this.dryRun {
		this.Ui.Output(fmt.Sprintf("mkdir %s/logs and chown to %s",
			this.instanceDir(), this.runAs))
	}
	err = os.MkdirAll(fmt.Sprintf("%s/logs", this.instanceDir()), 0755)
	swallow(err)
	chown(fmt.Sprintf("%s/logs", this.instanceDir()), this.userInfo)

	invalidDir := this.validateLogDirs(this.logDirs)
	if invalidDir != "" {
		this.Ui.Error(fmt.Sprintf("%s in log.dirs not exists!", invalidDir))
		return 2
	}

	// prepare the root directory
	this.rootPah = strings.TrimSuffix(this.rootPah, "/")
	if this.dryRun {
		this.Ui.Output(fmt.Sprintf("mkdir %s/bin and chown to %s",
			this.instanceDir(), this.runAs))
	}
	err = os.MkdirAll(fmt.Sprintf("%s/bin", this.instanceDir()), 0755)
	swallow(err)
	chown(fmt.Sprintf("%s/bin", this.instanceDir()), this.userInfo)

	if this.dryRun {
		this.Ui.Output(fmt.Sprintf("mkdir %s/config and chown to %s",
			this.instanceDir(), this.runAs))
	}
	err = os.MkdirAll(fmt.Sprintf("%s/config", this.instanceDir()), 0755)
	swallow(err)
	chown(fmt.Sprintf("%s/config", this.instanceDir()), this.userInfo)

	if this.influxDbAddr != "" {
		this.influxDbHost, this.influxdbPort, err = net.SplitHostPort(this.influxDbAddr)
		if err != nil {
			this.Ui.Error(err.Error())
			return 2
		}
		if this.influxDbHost == "" || this.influxdbPort == "" {
			this.Ui.Error("empty influxdb host or port")
			return 2
		}
	}

	influxReporterEnabled := "false"
	if this.influxDbHost != "" {
		influxReporterEnabled = "true"
	}
	data := templateVar{
		ZkAddrs:               this.zkzone.ZkAddrs(),
		ZkChroot:              zkchroot,
		KafkaBase:             this.kafkaBaseDir,
		BrokerId:              this.brokerId,
		Ip:                    this.ip,
		InstanceDir:           this.instanceDir(),
		User:                  this.runAs,
		TcpPort:               this.tcpPort,
		LogDirs:               this.logDirs,
		InfluxReporterEnabled: influxReporterEnabled,
		InfluxDbHost:          this.influxDbHost,
		InfluxDbPort:          this.influxdbPort,
	}
	data.IoThreads = strconv.Itoa(3 * len(strings.Split(data.LogDirs, ",")))
	networkThreads := ctx.NumCPU() / 2
	if networkThreads < 2 {
		networkThreads = 2
	}
	data.NetworkThreads = strconv.Itoa(networkThreads) // TODO not used yet

	// create the log.dirs directory and chown to sre
	logDirs := strings.Split(this.logDirs, ",")
	for _, logDir := range logDirs {
		if this.dryRun {
			this.Ui.Output(fmt.Sprintf("mkdir %s and chown to %s",
				logDir, this.runAs))
		}

		swallow(os.MkdirAll(logDir, 0755))
		chown(logDir, this.userInfo)
	}

	// package the kafka runtime together
	if !gio.DirExists(this.kafkaLibDir()) {
		this.installKafka()
	}

	// bin
	writeFileFromTemplate("template/bin/kafka-topics.sh",
		fmt.Sprintf("%s/bin/kafka-topics.sh", this.instanceDir()), 0755, nil, this.userInfo)
	writeFileFromTemplate("template/bin/kafka-reassign-partitions.sh",
		fmt.Sprintf("%s/bin/kafka-reassign-partitions.sh", this.instanceDir()), 0755, nil, this.userInfo)
	writeFileFromTemplate("template/bin/kafka-preferred-replica-election.sh",
		fmt.Sprintf("%s/bin/kafka-preferred-replica-election.sh", this.instanceDir()), 0755, nil, this.userInfo)

	writeFileFromTemplate("template/bin/kafka-run-class.sh",
		fmt.Sprintf("%s/bin/kafka-run-class.sh", this.instanceDir()), 0755, data, this.userInfo)
	writeFileFromTemplate("template/bin/kafka-server-start.sh",
		fmt.Sprintf("%s/bin/kafka-server-start.sh", this.instanceDir()), 0755, data, this.userInfo)
	writeFileFromTemplate("template/bin/setenv.sh",
		fmt.Sprintf("%s/bin/setenv.sh", this.instanceDir()), 0755, data, this.userInfo)

	// /etc/init.d/
	writeFileFromTemplate("template/init.d/kafka",
		fmt.Sprintf("/etc/init.d/%s", this.clusterName()), 0755, data, nil)

	// config
	writeFileFromTemplate("template/config/server.properties",
		fmt.Sprintf("%s/config/server.properties", this.instanceDir()), 0644, data, this.userInfo)
	writeFileFromTemplate("template/config/log4j.properties",
		fmt.Sprintf("%s/config/log4j.properties", this.instanceDir()), 0644, data, this.userInfo)

	this.Ui.Warn(fmt.Sprintf("NOW, please run the following command:"))
	this.Ui.Output(color.Red("chkconfig --add %s", this.clusterName()))
	this.Ui.Output(color.Red("/etc/init.d/%s start", this.clusterName()))

	return
}

func (this *Deploy) kafkaLibDir() string {
	return fmt.Sprintf("%s/libs", this.kafkaBaseDir)
}

func (this *Deploy) kafkaBinDir() string {
	return fmt.Sprintf("%s/bin", this.kafkaBaseDir)
}

func (this *Deploy) instanceDir() string {
	return fmt.Sprintf("%s/%s", this.rootPah, this.clusterName())
}

func (this *Deploy) clusterName() string {
	return fmt.Sprintf("kfk_%s", this.cluster)
}

func (*Deploy) validateLogDirs(dirs string) (invalidDir string) {
	for _, dir := range strings.Split(dirs, ",") {
		normDir := strings.TrimRight(dir, "/")
		parent := path.Dir(normDir)
		if !gio.DirExists(parent) {
			invalidDir = dir
			return // return on 1st invalid dir found
		}
	}

	return
}

func (this *Deploy) installHelix() {
	this.Ui.Output("installing helix runtime...")

	swallow(os.MkdirAll("/var/wd/helix/bin", 0755))
	swallow(os.MkdirAll("/var/wd/helix/repo", 0755))
	swallow(os.MkdirAll("/var/wd/helix/conf", 0755))

	// bin
	dir := fmt.Sprintf("template/helix-core-0.6.7/bin")
	files, err := AssetDir(dir)
	swallow(err)
	for _, file := range files {
		writeFileFromTemplate(
			fmt.Sprintf("%s/%s", dir, file),
			fmt.Sprintf("/var/wd/helix/bin/%s", file),
			0755, nil, nil)
	}

	// conf
	dir = fmt.Sprintf("template/helix-core-0.6.7/conf")
	files, err = AssetDir(dir)
	swallow(err)
	for _, file := range files {
		writeFileFromTemplate(
			fmt.Sprintf("%s/%s", dir, file),
			fmt.Sprintf("/var/wd/helix/conf/%s", file),
			0644, nil, nil)
	}

	// repo
	dir = fmt.Sprintf("template/helix-core-0.6.7/repo")
	for _, file := range []string{
		"com/101tec/zkclient/0.5/zkclient-0.5.jar",
		"com/google/guava/guava/15.0/guava-15.0.jar",
		"commons-cli/commons-cli/1.2/commons-cli-1.2.jar",
		"commons-codec/commons-codec/1.6/commons-codec-1.6.jar",
		"commons-io/commons-io/1.4/commons-io-1.4.jar",
		"io/netty/netty/3.10.5.Final/netty-3.10.5.Final.jar",
		"jline/jline/0.9.94/jline-0.9.94.jar",
		"log4j/log4j/1.2.15/log4j-1.2.15.jar",
		"org/codehaus/jackson/jackson-core-asl/1.8.5/jackson-core-asl-1.8.5.jar",
		"org/codehaus/jackson/jackson-mapper-asl/1.8.5/jackson-mapper-asl-1.8.5.jar",
		"org/slf4j/slf4j-api/1.6.1/slf4j-api-1.6.1.jar",
		"org/slf4j/slf4j-log4j12/1.6.1/slf4j-log4j12-1.6.1.jar",
		"org/yaml/snakeyaml/1.12/snakeyaml-1.12.jar",
		"org/apache/commons/commons-math/2.1/commons-math-2.1.jar",
		"org/apache/helix/helix-core/0.6.7/helix-core-0.6.7.jar",
		"org/apache/zookeeper/zookeeper/3.4.9/zookeeper-3.4.9.jar",
	} {
		writeFileFromTemplate(
			fmt.Sprintf("%s/%s", dir, file),
			fmt.Sprintf("/var/wd/helix/repo/%s", file),
			0644, nil, nil)
	}

	this.Ui.Info("helix runtime installed")
	this.Ui.Warn("yum install -y jdk-1.7.0_65-fcs.x86_64")
}

func (this *Deploy) installKafka() {
	this.Ui.Output("installing kafka runtime...")

	swallow(os.MkdirAll(this.kafkaLibDir(), 0755))
	swallow(os.MkdirAll(this.kafkaBinDir(), 0755))

	// install kafka libs
	kafkaLibTemplateDir := fmt.Sprintf("template/kafka_%s/libs", this.kafkaVer)
	jars, err := AssetDir(kafkaLibTemplateDir)
	swallow(err)
	for _, jar := range jars {
		writeFileFromTemplate(
			fmt.Sprintf("%s/%s", kafkaLibTemplateDir, jar),
			fmt.Sprintf("%s/libs/%s", this.kafkaBaseDir, jar),
			0644, nil, nil)
	}

	// install kafka bin
	kafkaBinTemplateDir := fmt.Sprintf("template/kafka_%s/bin", this.kafkaVer)
	scripts, err := AssetDir(kafkaBinTemplateDir)
	swallow(err)
	for _, script := range scripts {
		writeFileFromTemplate(
			fmt.Sprintf("%s/%s", kafkaBinTemplateDir, script),
			fmt.Sprintf("%s/bin/%s", this.kafkaBaseDir, script),
			0755, nil, nil)
	}

	this.Ui.Info("kafka runtime installed")
	this.Ui.Warn("yum install -y jdk-1.7.0_65-fcs.x86_64")
}

func (this *Deploy) demo() {
	var (
		maxPort int

		myPort     = -1
		myBrokerId = -1
	)

	this.zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		maxBrokerId := -1
		for _, broker := range liveBrokers {
			if maxPort < broker.Port {
				maxPort = broker.Port
			}

			if cluster == this.cluster {
				myPort = broker.Port

				bid, _ := strconv.Atoi(broker.Id)
				if bid > maxBrokerId {
					maxBrokerId = bid
				}

				myBrokerId = maxBrokerId + 1 // next deployable broker id
			}
		}

	})

	ip, err := ctx.LocalIP()
	swallow(err)

	if myPort == -1 {
		// the 1st deployment of this cluster
		myPort = maxPort + 1
	}
	if myBrokerId == -1 {
		// 1st broker id starts with 0
		myBrokerId = 0
	}
	logDirs := make([]string, 0)
	for i := 0; i <= 15; i++ {
		logDir := fmt.Sprintf("/data%d/%s", i, this.clusterName())
		if gio.DirExists(filepath.Dir(logDir)) {
			logDirs = append(logDirs, logDir)
		}
	}
	if len(logDirs) == 0 {
		// deploy on a small disk host, having no /dataX dirs
		logDirs = []string{fmt.Sprintf("%s/logs", this.instanceDir())}
	}

	influxAddr := ctx.Zone(this.zone).InfluxAddr
	if influxAddr != "" {
		this.Ui.Output(fmt.Sprintf("gk deploy -z %s -c %s -broker.id %d -port %d -ip %s -log.dirs %s -influx %s",
			this.zone, this.cluster,
			myBrokerId, myPort,
			ip.String(),
			strings.Join(logDirs, ","),
			influxAddr))
	} else {
		this.Ui.Output(fmt.Sprintf("gk deploy -z %s -c %s -broker.id %d -port %d -ip %s -log.dirs %s",
			this.zone, this.cluster,
			myBrokerId, myPort,
			ip.String(),
			strings.Join(logDirs, ",")))
	}

}

func (*Deploy) Synopsis() string {
	return "Deploy a new kafka broker on localhost"
}

func (this *Deploy) Help() string {
	help := fmt.Sprintf(`
Usage: %s deploy -z zone -c cluster [options]

    %s

Options:

    -kfkonly
      Only install kafka runtime on localhost.

    -helix
      Install helix only on localhost.

    -influx host:port
      InfluxDB server address used for kafka metrics reporter.
      If empty, InfluxDB metrics reporter will be disabled.

    -uninstall base dir
      Uninstall a kafka broker on localhost.

    -demo
      Demonstrate how to use this command.

    -diff
      Compare the new deployment config with the installed kafka on localhost.

    -dryrun
      Default is true.

    -root dir
      Root directory of the kafka broker.
      Defaults to /var/wd

    -ip addr
      Advertised host name of this new broker.	

    -port port
      Tcp port the broker will listen on.

    -broker.id id

    -user runAsUser
      The deployed kafka broker will run as this user.
      Defaults to sre

    -ver [2.10-0.8.1.1|2.10-0.8.2.2]
      Defaults to 2.10-0.8.2.2

    -kafka.base dir
      Kafka installation prefix dir.
      Defaults to %s

    -log.dirs dirs
      A comma separated list of directories under which to store log files.

`, this.Cmd, this.Synopsis(), ctx.KafkaHome())
	return strings.TrimSpace(help)
}
