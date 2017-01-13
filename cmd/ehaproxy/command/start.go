package command

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/registry"
	zkr "github.com/funkygao/gafka/registry/zk"
	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/gafka/telemetry/influxdb"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	"github.com/funkygao/gocli"
	gio "github.com/funkygao/golib/io"
	"github.com/funkygao/golib/locking"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

type Start struct {
	Ui  cli.Ui
	Cmd string

	startedAt time.Time

	zone       string
	root       string
	debugMode  bool
	command    string
	logfile    string
	pubPort    int
	subPort    int
	manPort    int
	starting   bool
	forwardFor bool
	httpAddr   string

	haproxyStatsUrl string
	influxdbAddr    string
	influxdbDbName  string

	quitCh, closed chan struct{}
	zkzone         *zk.ZkZone
	lastServers    BackendServers
}

func (this *Start) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("start", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.logfile, "log", defaultLogfile, "")
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.root, "p", defaultPrefix, "")
	cmdFlags.BoolVar(&this.debugMode, "d", false, "")
	cmdFlags.BoolVar(&this.forwardFor, "forwardfor", false, "")
	cmdFlags.IntVar(&this.pubPort, "pub", 10891, "")
	cmdFlags.IntVar(&this.subPort, "sub", 10892, "")
	cmdFlags.IntVar(&this.manPort, "man", 10893, "")
	cmdFlags.StringVar(&this.haproxyStatsUrl, "statsurl", "", "")
	cmdFlags.StringVar(&this.influxdbAddr, "influxaddr", "", "")
	cmdFlags.StringVar(&this.influxdbDbName, "influxdb", "", "")
	cmdFlags.StringVar(&this.httpAddr, "addr", ":10894", "monitor http server addr")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.debugMode {
		this.pubPort = 9191
		this.subPort = 9192
		this.manPort = 9193
	}

	lockFilename := fmt.Sprintf("%s/.lock", this.root)
	if locking.InstanceLocked(lockFilename) {
		panic(fmt.Sprintf("locked[%s] by another instance", lockFilename))
	}

	locking.LockInstance(lockFilename)

	err := os.Chdir(this.root)
	swalllow(err)

	this.command = fmt.Sprintf("%s/sbin/haproxy", this.root)
	if _, err := os.Stat(this.command); err != nil {
		panic(err)
	}

	this.setupLogging(this.logfile, "info", "panic")
	this.starting = true
	this.startedAt = time.Now()

	if this.haproxyStatsUrl != "" &&
		this.influxdbAddr != "" && this.influxdbDbName != "" {
		rc, err := influxdb.NewConfig(this.influxdbAddr, this.influxdbDbName, "", "", time.Minute)
		if err != nil {
			panic(err)
		}
		telemetry.Default = influxdb.New(metrics.DefaultRegistry, rc)
		go func() {
			log.Info("telemetry started: %s", telemetry.Default.Name())

			if err := telemetry.Default.Start(); err != nil {
				log.Error("telemetry: %v", err)
			}
		}()

		m := haproxyMetrics{
			ctx:      this,
			interval: time.Second * 30,
			uri:      this.haproxyStatsUrl,
		}
		go m.start()
	}

	this.quitCh = make(chan struct{})
	this.closed = make(chan struct{})
	signal.RegisterHandler(func(sig os.Signal) {
		log.Info("ehaproxy[%s] got signal: %s", gafka.BuildId, strings.ToUpper(sig.String()))
		this.shutdown()

		log.Info("removing %s", configFile)
		os.Remove(configFile)

		log.Info("removing lock[%s]", lockFilename)
		locking.UnlockInstance(lockFilename)

		close(this.quitCh)

		if telemetry.Default != nil {
			log.Info("stopping telemetry and flush all metrics...")
			telemetry.Default.Stop()
		}

		log.Info("ehaproxy[%s] shutdown complete", gafka.BuildId)
		log.Info("ehaproxy[%s] %s bye!", gafka.BuildId, time.Since(this.startedAt))

		close(this.closed)
	}, syscall.SIGINT, syscall.SIGTERM)

	this.main()

	<-this.closed
	log.Close()

	return
}

func (this *Start) main() {
	ctx.LoadFromHome()
	this.zkzone = zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkConnEvt, ok := this.zkzone.SessionEvents()
	if !ok {
		panic("someone stealing my events")
	}

	registry.Default = zkr.New(this.zkzone)

	log.Info("ehaproxy[%s] starting...", gafka.BuildId)
	go this.runMonitorServer(this.httpAddr)

	zkConnected := false
	for {
		instances, instancesChange, err := registry.Default.WatchInstances()
		if err != nil {
			log.Error("zone[%s] %s", this.zkzone.Name(), err)
			time.Sleep(time.Second)
			continue
		}

		if zkConnected {
			if len(instances) > 0 {
				this.reload(instances)
			} else {
				// resilience to zk problem by local cache
				log.Warn("backend all shutdown? skip this change")
				time.Sleep(time.Second)
				continue
			}
		}

		select {
		case <-this.quitCh:
			return

		case evt := <-zkConnEvt:
			if evt.State == zklib.StateHasSession && !zkConnected {
				log.Info("zk connected")
				zkConnected = true
			} else if zkConnected && evt.Path == "" {
				log.Warn("zk jitter: %+v", evt)
			}

		case <-instancesChange:
			log.Info("instances changed!!")
		}
	}

}

func (this *Start) reload(kwInstances []string) {
	var servers = BackendServers{
		CpuNum:      ctx.NumCPU(),
		HaproxyRoot: this.root,
		ForwardFor:  this.forwardFor,
		PubPort:     this.pubPort,
		SubPort:     this.subPort,
		ManPort:     this.manPort,
	}
	servers.reset()
	for _, kwNode := range kwInstances {
		data, _, err := this.zkzone.Conn().Get(kwNode)
		if err != nil {
			log.Error("%s: %v", kwNode, err)
			continue
		}

		info := make(map[string]string)
		if err = json.Unmarshal([]byte(data), &info); err != nil {
			log.Error("%s: %v", data, err)
			continue
		}

		// pub
		if info["pub"] != "" {
			_, port, _ := net.SplitHostPort(info["pub"])
			be := Backend{
				Name: "p" + info["id"],
				Addr: info["pub"],
				Cpu:  info["cpu"],
				Port: port,
			}
			servers.Pub = append(servers.Pub, be)
		}

		// sub
		if info["sub"] != "" {
			_, port, _ := net.SplitHostPort(info["sub"])
			be := Backend{
				Name: "s" + info["id"],
				Addr: info["sub"],
				Cpu:  info["cpu"],
				Port: port,
			}
			servers.Sub = append(servers.Sub, be)
		}

		// man
		if info["man"] != "" {
			_, port, _ := net.SplitHostPort(info["man"])
			be := Backend{
				Name: "m" + info["id"],
				Addr: info["man"],
				Cpu:  info["cpu"],
				Port: port,
			}
			servers.Man = append(servers.Man, be)
		}
	}

	for i := 0; i < ctx.NumCPU(); i++ {
		servers.Dashboard = append(servers.Dashboard, Backend{
			Port: fmt.Sprintf("%d", dashboardPortHead+i),
			Name: fmt.Sprintf("%d", i+1), // process id starts from 1
		})
	}

	if servers.empty() {
		log.Warn("empty backend servers, all shutdown?")
		return
	}

	if reflect.DeepEqual(this.lastServers, servers) {
		log.Warn("backend servers stays unchanged")
		return
	}

	this.lastServers = servers
	if err := this.createConfigFile(servers); err != nil {
		log.Error(err)
		return
	}

	if err := this.reloadHAproxy(); err != nil {
		log.Error("reloading haproxy: %v", err)
		panic(err)
	}
}

func (this *Start) shutdown() {
	// kill haproxy
	log.Info("killling haproxy processes")

	f, e := os.Open(haproxyPidFile)
	if e != nil {
		log.Error("shutdown %v", e)
		return
	}

	reader := bufio.NewReader(f)
	for {
		l, e := gio.ReadLine(reader)
		if e != nil {
			// EOF
			break
		}

		pid, _ := strconv.Atoi(string(l))
		p := &os.Process{
			Pid: pid,
		}
		if err := p.Kill(); err != nil {
			log.Error(err)
		} else {
			log.Info("haproxy[%d] terminated", pid)
		}
	}

	log.Info("removing %s", haproxyPidFile)
	os.Remove(haproxyPidFile)
}

func (this *Start) Synopsis() string {
	return fmt.Sprintf("Start %s system on localhost", this.Cmd)
}

func (this *Start) Help() string {
	help := fmt.Sprintf(`
Usage: %s start [options]

    Start %s system on localhost

Options:

    -z zone
      Default %s

    -d
      Debug mode

    -statsurl url
      haproxy stats url

    -influxaddr addr

    -influxdb dbName

    -forwardfor
      Default false.
      If true, haproxy will add X-Forwarded-For http header.

    -pub pub server listen port

    -sub sub server listen port

    -man manager server listen port

    -p directory prefix
      Default %s

    -log log file
      Default %s

`, this.Cmd, this.Cmd, ctx.ZkDefaultZone(), defaultPrefix, defaultLogfile)
	return strings.TrimSpace(help)
}
