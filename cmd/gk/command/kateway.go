package command

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/golib/pipestream"
	"github.com/funkygao/golib/stress"
	"github.com/pborman/uuid"
	"github.com/ryanuber/columnize"
	zklib "github.com/samuel/go-zookeeper/zk"
)

type Kateway struct {
	Ui  cli.Ui
	Cmd string

	zone            string
	id              string
	configMode      bool
	logLevel        string
	configOption    string
	longFmt         bool
	install         bool
	resetCounter    string
	visualLog       string
	checkup         bool
	curl            bool
	pub, sub        bool
	versionOnly     bool
	flameGraph      bool
	benchmark       bool
	benchmarkAsync  bool
	benchmarkMaster string
	showZkNodes     bool

	benchApp, benchSecret, benchTopic, benchVer, benchPubEndpoint string
	benchId                                                       string
}

func (this *Kateway) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("kateway", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.BoolVar(&this.configMode, "cf", false, "")
	cmdFlags.StringVar(&this.id, "id", "", "")
	cmdFlags.BoolVar(&this.install, "i", false, "")
	cmdFlags.BoolVar(&this.longFmt, "l", false, "")
	cmdFlags.StringVar(&this.configOption, "option", "", "")
	cmdFlags.BoolVar(&this.versionOnly, "ver", false, "")
	cmdFlags.BoolVar(&this.flameGraph, "flame", false, "")
	cmdFlags.StringVar(&this.logLevel, "loglevel", "", "")
	cmdFlags.StringVar(&this.visualLog, "visualog", "", "")
	cmdFlags.BoolVar(&this.showZkNodes, "zk", false, "")
	cmdFlags.BoolVar(&this.checkup, "checkup", false, "")
	cmdFlags.BoolVar(&this.benchmark, "bench", false, "")
	cmdFlags.StringVar(&this.benchmarkMaster, "master", "", "")
	cmdFlags.BoolVar(&this.pub, "pub", false, "")
	cmdFlags.BoolVar(&this.sub, "sub", false, "")
	cmdFlags.BoolVar(&this.benchmarkAsync, "async", false, "")
	cmdFlags.BoolVar(&this.curl, "curl", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	if this.benchmark {
		if validateArgs(this, this.Ui).
			require("-z").
			requireAdminRights("-z").
			invalid(args) {
			return 2
		}

		zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
		zone := ctx.Zone(zkzone.Name())
		this.benchApp = zone.SmokeApp
		this.benchSecret = zone.SmokeSecret
		this.benchTopic = zone.SmokeTopic
		this.benchVer = zone.SmokeTopicVersion
		this.benchPubEndpoint = zone.PubEndpoint
		if this.id != "" {
			kws, err := zkzone.KatewayInfos()
			swallow(err)
			for _, kw := range kws {
				if kw.Id == this.id {
					this.benchPubEndpoint = kw.PubAddr
					break
				}
			}
		}
		this.benchId = fmt.Sprintf("%s-%s", ctx.Hostname(), strings.Replace(uuid.New(), "-", "", -1))
		this.runBenchmark(zkzone)
		return
	}

	if this.flameGraph {
		if validateArgs(this, this.Ui).
			require("-z", "-id").
			requireAdminRights("-z").
			invalid(args) {
			return 2
		}

		zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
		this.generateFlameGraph(zkzone)
		return
	}

	if this.visualLog != "" {
		this.doVisualize()
		return
	}

	if this.pub {
		zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
		this.runPub(zkzone)
		return
	}

	if this.sub {
		zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
		this.runSub(zkzone)
		return
	}

	if this.install {
		if validateArgs(this, this.Ui).
			require("-z").
			invalid(args) {
			return 2
		}

		zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
		this.installGuide(zkzone)
		return
	}

	if this.configOption != "" {
		this.configMode = true
	}

	if this.configMode {
		if validateArgs(this, this.Ui).
			require("-z").
			requireAdminRights("-z").
			invalid(args) {
			return 2
		}

		zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
		if this.logLevel != "" {
			if this.id != "" {
				kw := zkzone.KatewayInfoById(this.id)
				if kw == nil {
					panic(fmt.Sprintf("kateway %s invalid entry found in zk", this.id))
				}

				this.callKateway(kw, "PUT", fmt.Sprintf("v1/log/%s", this.logLevel))
			} else {
				// apply on all kateways
				kws, _ := zkzone.KatewayInfos()
				for _, kw := range kws {
					this.callKateway(kw, "PUT", fmt.Sprintf("v1/log/%s", this.logLevel))
				}
			}
		}

		if this.configOption != "" {
			parts := strings.SplitN(this.configOption, "=", 2)
			if len(parts) != 2 {
				this.Ui.Error("usage: key=value")
				return
			}
			k, v := parts[0], parts[1]
			if this.id != "" {
				kw := zkzone.KatewayInfoById(this.id)
				if kw == nil {
					panic(fmt.Sprintf("kateway %s invalid entry found in zk", this.id))
				}

				this.callKateway(kw, "PUT", fmt.Sprintf("v1/options/%s/%s", k, v))
			} else {
				// apply on all kateways
				kws, _ := zkzone.KatewayInfos()
				for _, kw := range kws {
					this.callKateway(kw, "PUT", fmt.Sprintf("v1/options/%s/%s", k, v))
				}
			}
		}

		return
	}

	if this.checkup {
		if this.zone == "" {
			forAllSortedZones(func(zkzone *zk.ZkZone) {
				this.runCheckup(zkzone)
			})

			return
		}

		zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
		this.runCheckup(zkzone)
		return
	}

	if this.showZkNodes {
		this.Ui.Output(fmt.Sprintf(`%s pubsub manager db dsn
%s job db cluster config
%s turn off webhook dir`,
			color.Green("%-50s", zk.KatewayMysqlPath),
			color.Green("%-50s", zk.PubsubJobConfig),
			color.Green("%-50s", zk.PubsubWebhooksOff)))
		return
	}

	// display mode
	lines := make([]string, 0)
	header := "Zone|Id|Ip|Pprof|Build|Cpu|Heap|Obj|Go|P/S/hhIn/hhOut|Uptime"
	lines = append(lines, header)
	forSortedZones(func(zkzone *zk.ZkZone) {
		if this.zone != "" && zkzone.Name() != this.zone {
			return
		}

		if !this.versionOnly {
			mysqlDsn, err := zkzone.KatewayMysqlDsn()
			if err != nil {
				this.Ui.Warn(fmt.Sprintf("kateway[%s] mysql DSN not set on zk yet", zkzone.Name()))
				this.Ui.Output(fmt.Sprintf("e,g. %s -> pubsub:pubsub@tcp(10.77.135.217:10010)/pubsub?charset=utf8&timeout=10s",
					zk.KatewayMysqlPath))
			} else {
				this.Ui.Output(fmt.Sprintf("zone[%s] manager db: %s",
					color.Cyan(zkzone.Name()), mysqlDsn))
			}
		}

		kateways, err := zkzone.KatewayInfos()
		if err != nil {
			if err == zklib.ErrNoNode {
				this.Ui.Output("no kateway running")
				return
			} else {
				swallow(err)
			}

		}

		for _, kw := range kateways {
			if this.id != "" && this.id != kw.Id {
				continue
			}

			statusMap, _ := this.getKatewayStatusMap(kw.ManAddr)
			logLevel, _ := statusMap["loglevel"].(string)
			heapSize, _ := statusMap["heap"].(string)
			heapObjs, _ := statusMap["objects"].(string)
			pubConn, _ := statusMap["pubconn"].(string)
			hhAppendN, _ := statusMap["hh_appends"].(string)
			hhDeliverN, _ := statusMap["hh_delivers"].(string)
			subConn, _ := statusMap["subconn"].(string)
			goN, _ := statusMap["goroutines"].(string)

			if this.versionOnly {
				pprofAddr := kw.DebugAddr
				if len(pprofAddr) > 0 && pprofAddr[0] == ':' {
					pprofAddr = kw.Ip + pprofAddr
				}
				pprofAddr = fmt.Sprintf("%s/debug/pprof/", pprofAddr)
				lines = append(lines, fmt.Sprintf("%s|%s|%s|%s|%s/%s|%s|%s|%s|%s|%s/%s/%s/%s|%s",
					zkzone.Name(),
					kw.Id, kw.Ip,
					pprofAddr, kw.Build, kw.BuiltAt,
					kw.Cpu,
					heapSize, heapObjs,
					goN,
					pubConn, subConn, hhAppendN, hhDeliverN,
					gofmt.PrettySince(kw.Ctime)))
				continue
			}

			this.Ui.Info(fmt.Sprintf("id:%-2s host:%s cpu:%-2s up:%s",
				kw.Id, kw.Host, kw.Cpu,
				gofmt.PrettySince(kw.Ctime)))
			this.Ui.Output(fmt.Sprintf("    ver: %s\n   arch: %s\n  build: %s\n  built: %s\n    log: %s\n    pub: %s\n    sub: %s\n    man: %s\n    dbg: %s",
				kw.Ver,
				kw.Arch,
				color.Red(kw.Build),
				kw.BuiltAt,
				logLevel,
				kw.PubAddr,
				kw.SubAddr,
				kw.ManAddr,
				kw.DebugAddr,
			))

			if this.longFmt {
				this.Ui.Output("    full status:")
				this.Ui.Output(this.getKatewayStatus(kw.ManAddr))
			}

		}

	})

	if this.versionOnly && len(lines) > 1 {
		fmt.Println(columnize.SimpleFormat(lines))
	}

	return
}

func (this *Kateway) installGuide(zkzone *zk.ZkZone) {
	this.Ui.Output(color.Red("manager db GRANT access rights to this ip"))
	this.Ui.Output(color.Red("gk deploy -kfkonly"))
	this.Ui.Output("")

	this.Ui.Output("mkdir -p /var/wd/kateway/sbin")
	this.Ui.Output("cd /var/wd/kateway")
	kateways, err := zkzone.KatewayInfos()
	swallow(err)
	nextId := 1
	for _, kw := range kateways {
		id, _ := strconv.Atoi(kw.Id)
		if nextId < id {
			nextId = id
		}
	}
	nextId++

	zone := ctx.Zone(this.zone)
	influxAddr := zone.InfluxAddr
	if influxAddr != "" && !strings.HasPrefix(influxAddr, "http://") {
		influxAddr = "http://" + influxAddr
	}
	var influxInfo string
	if influxAddr != "" {
		influxInfo = "-influxdbaddr " + influxAddr
	}

	this.Ui.Output(fmt.Sprintf(`nohup ./sbin/kateway -zone prod -id %d -debughttp ":10194" -level trace -log kateway.log -crashlog panic %s &`,
		nextId, influxInfo))
	this.Ui.Output("")

	this.Ui.Output("yum install -y logstash")
	this.Ui.Output("/etc/logstash/conf.d/kateway.conf")
	this.Ui.Output(strings.TrimSpace(fmt.Sprintf(`
input {
    file {
        path => "/var/wd/kateway/kateway.log"
        type => "kateway"
    }
    file {
        path => "/var/wd/kateway/panic"
        type => "kateway_panic"
    }
}

output {
    kafka {
        bootstrap_servers => "%s:11003,%s:11003"
        topic_id => "pubsub_log"
    }
}
		`, color.Red("k11003a.mycorp.kfk.com"), color.Red("k11003b.mycorp.kfk.com"))))
	this.Ui.Output("")

	this.Ui.Output("chkconfig --add logstash")
	this.Ui.Output("/etc/init.d/logstash start")
}

func (this Kateway) getKatewayStatus(url string) string {
	url = fmt.Sprintf("http://%s/v1/status", url)
	body, err := this.callHttp(url, "GET")
	if err != nil {
		return err.Error()
	}

	return string(body)
}

func (this *Kateway) getKatewayStatusMap(url string) (map[string]interface{}, error) {
	url = fmt.Sprintf("http://%s/v1/status", url)
	body, err := this.callHttp(url, "GET")
	if err != nil {
		return nil, err
	}

	var v map[string]interface{}
	err = json.Unmarshal(body, &v)
	return v, err
}

func (this *Kateway) callHttp(url string, method string) (body []byte, err error) {
	var req *http.Request
	req, err = http.NewRequest(method, url, nil)
	if err != nil {
		return
	}

	var response *http.Response
	timeout := time.Second * 10
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1,
			Proxy:               http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout: timeout,
			}).Dial,
			DisableKeepAlives:     true,
			ResponseHeaderTimeout: timeout,
			TLSHandshakeTimeout:   timeout,
		},
	}

	response, err = client.Do(req)
	if err != nil {
		return
	}

	body, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}

	response.Body.Close()

	if response.StatusCode != http.StatusOK {
		this.Ui.Error(fmt.Sprintf("%s %s %s", url, response.Status, string(body)))
	}

	return
}

func (this *Kateway) callKateway(kw *zk.KatewayMeta, method string, uri string) (err error) {
	url := fmt.Sprintf("http://%s/%s", kw.ManAddr, uri)
	var body []byte
	body, err = this.callHttp(url, method)
	this.Ui.Output(fmt.Sprintf("id[%s] -> %s", kw.Id, string(body)))
	return
}

func (this *Kateway) runBenchmark(zkzone *zk.ZkZone) {
	this.Ui.Info(fmt.Sprintf("benchmark[%s] zone[%s] %s.%s.%s %s",
		this.benchId, zkzone.Name(),
		this.benchApp, this.benchTopic, this.benchVer, this.benchPubEndpoint))
	yes, _ := this.Ui.Ask("Are you sure to execute the benchmark? [Y/N]")
	if yes == "Y" {
		log.SetOutput(os.Stdout)
		stress.Flags.Round = 5
		stress.Flags.Tick = 5
		if this.benchmarkMaster != "" {
			stress.Flags.MasterAddr = stress.MasterAddr(this.benchmarkMaster)
		}
		stress.RunStress(this.benchPub)
	}

}

func (this *Kateway) benchPub(seq int) {
	cf := api.DefaultConfig(this.benchApp, this.benchSecret)
	cf.Pub.Endpoint = this.benchPubEndpoint
	cli := api.NewClient(cf)
	for i := 0; i < 10000; i++ {
		pubMsg := fmt.Sprintf("gk kateway -bench generated by %s %d.%d", this.benchId, seq, i)
		if err := cli.Pub("", []byte(pubMsg), api.PubOption{
			Topic: this.benchTopic,
			Ver:   this.benchVer,
			Async: this.benchmarkAsync,
		}); err != nil {
			log.Printf("%s/%d/%d %s", this.benchId, seq, i, err)
			stress.IncCounter("fail", 1)
		} else {
			stress.IncCounter("ok", 1)
		}
	}

}

func (this *Kateway) runPub(zkzone *zk.ZkZone) {
	zone := ctx.Zone(zkzone.Name())
	cf := api.DefaultConfig(zone.SmokeApp, zone.SmokeSecret)
	cf.Pub.Endpoint = zone.PubEndpoint
	cli := api.NewClient(cf)

	for {
		now := time.Now()
		pubMsg := fmt.Sprintf("gk kateway -pub smoke test msg: [%s]", now)
		err := cli.Pub("", []byte(pubMsg), api.PubOption{
			Topic: zone.SmokeTopic,
			Ver:   zone.SmokeTopicVersion,
		})
		this.Ui.Output(fmt.Sprintf("<- %s: %s %v", time.Since(now), pubMsg, err))

		time.Sleep(time.Millisecond * 100)
	}
}

func (this *Kateway) runSub(zkzone *zk.ZkZone) {
	zone := ctx.Zone(zkzone.Name())
	cf := api.DefaultConfig(zone.SmokeApp, zone.SmokeSecret)
	cf.Sub.Endpoint = zone.SubEndpoint
	cli := api.NewClient(cf)

	t1 := time.Now()
	err := cli.Sub(api.SubOption{
		AppId:     zone.SmokeHisApp,
		Topic:     zone.SmokeTopic,
		Ver:       zone.SmokeTopicVersion,
		Group:     zone.SmokeGroup,
		AutoClose: false,
	}, func(statusCode int, subMsg []byte) error {
		now := time.Now()
		var e error
		if statusCode != http.StatusOK {
			e = fmt.Errorf("unexpected http status: %s, body:%s", http.StatusText(statusCode), string(subMsg))
		}
		this.Ui.Output(fmt.Sprintf("-> %s: %s %v", now.Sub(t1), string(subMsg), e))

		time.Sleep(time.Millisecond * 100)
		t1 = time.Now()

		return e
	})

	if err != nil {
		this.Ui.Error(err.Error())
	}
}

func (this *Kateway) runCheckup(zkzone *zk.ZkZone) {
	zone := ctx.Zone(zkzone.Name())
	var (
		myApp  = zone.SmokeApp
		hisApp = zone.SmokeHisApp
		secret = zone.SmokeSecret
		ver    = zone.SmokeTopicVersion
		topic  = zone.SmokeTopic
		group  = zone.SmokeGroup
	)

	if myApp == "" || secret == "" {
		this.Ui.Warn(fmt.Sprintf("zone[%s] skipped", zkzone.Name()))
		return
	}

	rand.Seed(time.Now().UTC().UnixNano())

	kws, err := zkzone.KatewayInfos()
	swallow(err)
	if zone.PubEndpoint != "" && zone.SubEndpoint != "" {
		// add the load balancer endpoint
		kws = append(kws, &zk.KatewayMeta{
			Id:      "loadbalancer",
			PubAddr: zone.PubEndpoint,
			SubAddr: zone.SubEndpoint,
		})
	}

	for _, kw := range kws {
		if this.id != "" && kw.Id != this.id {
			continue
		}

		this.Ui.Info(fmt.Sprintf("zone[%s] kateway[%s]", zkzone.Name(), kw.Id))

		// pub a message
		cf := api.DefaultConfig(myApp, secret)
		cf.Pub.Endpoint = kw.PubAddr
		cf.Sub.Endpoint = kw.SubAddr
		cli := api.NewClient(cf)

		pubMsg := fmt.Sprintf("gk smoke test msg: [%s]", time.Now())

		if this.curl {
			this.Ui.Output(fmt.Sprintf(`curl -XPOST -H'Appid: %s' -H'Pubkey: %s' -d '%s' %s`,
				myApp, secret, pubMsg, fmt.Sprintf("http://%s/v1/msgs/%s/%s", kw.PubAddr, topic, ver)))
		}

		err = cli.Pub("", []byte(pubMsg), api.PubOption{
			Topic: topic,
			Ver:   ver,
		})
		swallow(err)

		if this.curl {
			this.Ui.Output(fmt.Sprintf(`curl -XGET -H'Appid: %s' -H'Subkey: %s' %s`,
				myApp, secret, fmt.Sprintf("http://%s/v1/msgs/%s/%s/%s?group=%s", kw.SubAddr, hisApp, topic, ver, group)))
		}

		// confirm that sub can get the pub'ed message
		err = cli.Sub(api.SubOption{
			AppId:     hisApp,
			Topic:     topic,
			Ver:       ver,
			Group:     group,
			AutoClose: true,
		}, func(statusCode int, subMsg []byte) error {
			if statusCode != http.StatusOK {
				return fmt.Errorf("unexpected http status: %s, body:%s", http.StatusText(statusCode), string(subMsg))
			}
			if len(subMsg) < 10 {
				this.Ui.Warn(fmt.Sprintf("unexpected sub msg: %s", string(subMsg)))
			}

			return api.ErrSubStop
		})
		swallow(err)

		this.Ui.Info(fmt.Sprintf("    ok for %s@%s", kw.Id, kw.Build))

		// wait for server cleanup the sub conn
		time.Sleep(time.Second)

		// 1. 查询某个pubsub topic的partition数量
		// 2. 查看pubsub系统某个topic的生产、消费状态
		// 3. pub
		// 4. sub
	}

}

func (this *Kateway) generateFlameGraph(zkzone *zk.ZkZone) {
	kws, _ := zkzone.KatewayInfos()
	for _, kw := range kws {
		if kw.Id != this.id {
			continue
		}

		pprofAddr := kw.DebugAddr
		if len(pprofAddr) > 0 && pprofAddr[0] == ':' {
			pprofAddr = kw.Ip + pprofAddr
		}
		pprofAddr = fmt.Sprintf("http://%s/debug/pprof/profile", pprofAddr)
		cmd := pipestream.New(os.Getenv("GOPATH")+"/bin/go-torch",
			"-u", pprofAddr)
		err := cmd.Open()
		swallow(err)
		defer cmd.Close()

		scanner := bufio.NewScanner(cmd.Reader())
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}

		this.Ui.Output("torch.svg generated")
	}

}

func (this *Kateway) doVisualize() {
	cmd := pipestream.New("/usr/local/bin/logstalgia", "-f", this.visualLog)
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
	}
}

func (*Kateway) Synopsis() string {
	return "List/Config online kateway instances"
}

func (this *Kateway) Help() string {
	help := fmt.Sprintf(`
Usage: %s kateway [options]

    %s

Options:

    -z zone

    -i
      Install kateway guide

    -bench
      Run Pub benchmark agaist kateway cluster

    -master
      Benchmark master ip address
      If empty, this host serves as master

    -async
      Run benchmark in async mode

    -pub
      Continously pub

    -sub
      Continously sub

    -flame
      Generate a kateway instance flame graph
      Must install go-torch and FlameGraph in advance
      e,g.
      gk kateway -z prod -id 1 -flame

    -ver
      Display kateway version only

    -zk
      Display kateway zk config nodes and exit

    -checkup
      Checkup for online kateway instances

    -curl
	  Display curl command for manually checkup
	  Use with checkup mode

    -visualog access log filename
      Visualize the kateway access log with Logstalgia
      You must install Logstalgia beforehand

    -id kateway id
      Execute on a single kateway instance. By default, apply on all

    -l
      Use a long listing format

    -cf
      Enter config mode
   
    -option <key>=<val>
      Set kateway options value
      keys:
      debug|gzip|badgroup_rater|badpub_rater|hh|hhflush|jobshardid|accesslog|punish|500backoff|loglevel|
      auditpub|refreshdb|auditsub|standbysub|unregroup|nometrics|resethh|ratelimit|maxreq|allhh

      e,g.
      refreshdb=true
      resethh=true
      badgroup_rater=true
      punish=3s
      allhh=true
      500backoff=2s
      maxreq=1000
      loglevel=<info|debug|trace|warn|alarm|error>

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
