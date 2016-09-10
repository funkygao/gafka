package command

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/golib/pipestream"
	"github.com/ryanuber/columnize"
	zklib "github.com/samuel/go-zookeeper/zk"
)

type Kateway struct {
	Ui  cli.Ui
	Cmd string

	zone         string
	id           string
	configMode   bool
	logLevel     string
	configOption string
	longFmt      bool
	install      bool
	resetCounter string
	visualLog    string
	checkup      bool
	versionOnly  bool
	showZkNodes  bool
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
	cmdFlags.StringVar(&this.resetCounter, "reset", "", "")
	cmdFlags.BoolVar(&this.versionOnly, "ver", false, "")
	cmdFlags.StringVar(&this.logLevel, "loglevel", "", "")
	cmdFlags.StringVar(&this.visualLog, "visualog", "", "")
	cmdFlags.BoolVar(&this.showZkNodes, "zk", false, "")
	cmdFlags.BoolVar(&this.checkup, "checkup", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	if this.visualLog != "" {
		this.doVisualize()
		return
	}

	if this.install {
		this.installGuide()
		return
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

		if this.resetCounter != "" {
			if this.id != "" {
				kw := zkzone.KatewayInfoById(this.id)
				if kw == nil {
					panic(fmt.Sprintf("kateway %s invalid entry found in zk", this.id))
				}

				this.callKateway(kw, "DELETE", fmt.Sprintf("v1/counter/%s", this.resetCounter))
			} else {
				// apply on all kateways
				kws, _ := zkzone.KatewayInfos()
				for _, kw := range kws {
					this.callKateway(kw, "DELETE", fmt.Sprintf("v1/counter/%s", this.resetCounter))
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
		if validateArgs(this, this.Ui).
			require("-z").
			requireAdminRights("-z").
			invalid(args) {
			return 2
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
	header := "Zone|Id|Host|Ip|Pprof|Build|Cpu|Mem|P/S|Uptime"
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
			logLevel := statusMap["loglevel"].(string)
			heapSize, ok := statusMap["heap"].(string)
			if !ok {
				heapSize = ""
			}
			pubConn, ok := statusMap["pubconn"].(string)
			if !ok {
				pubConn = ""
			}
			subConn, ok := statusMap["subconn"].(string)
			if !ok {
				subConn = ""
			}

			if this.versionOnly {
				pprofAddr := kw.DebugAddr
				if pprofAddr[0] == ':' {
					pprofAddr = kw.Ip + pprofAddr
				}
				pprofAddr = fmt.Sprintf("%s/debug/pprof/", pprofAddr)
				lines = append(lines, fmt.Sprintf("%s|%s|%s|%s|%s|%s/%s|%s|%s|%s/%s|%s",
					zkzone.Name(),
					kw.Id, kw.Host, kw.Ip,
					pprofAddr, kw.Build, kw.BuiltAt,
					kw.Cpu,
					heapSize,
					pubConn, subConn,
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
		this.Ui.Output(columnize.SimpleFormat(lines))
	}

	return
}

func (this *Kateway) installGuide() {
	this.Ui.Warn("FIRST: manager db GRANT access rights to this ip")
	this.Ui.Info("gk deploy -kfkonly")
	this.Ui.Info("mkdir -p /var/wd/kateway/sbin")
	this.Ui.Info("cd /var/wd/kateway")
	this.Ui.Info("nohup ./sbin/kateway -zone prod -id 1 -level trace -log kateway.log -crashlog panic -influxdbaddr http://10.213.1.223:8086 &")
	this.Ui.Info("")
	this.Ui.Info("yum install -y logstash")
	this.Ui.Info("/etc/logstash/conf.d/kateway.conf")
	this.Ui.Output(strings.TrimSpace(`
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
        broker_list => "k11003a.mycorp.kfk.com:11003,k11003b.mycorp.kfk.com:11003"
        topic_id => "pubsub_log"
        topic_metadata_refresh_interval_ms => 600000
    }
}
		`))
	this.Ui.Info("chkconfig --add logstash")
	this.Ui.Info("/etc/init.d/logstash start")
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

	rand.Seed(time.Now().UTC().UnixNano())

	kws, err := zkzone.KatewayInfos()
	swallow(err)
	for _, kw := range kws {
		if this.id != "" && kw.Id != this.id {
			continue
		}

		// pub a message
		cf := api.DefaultConfig(myApp, secret)
		cf.Pub.Endpoint = kw.PubAddr
		cf.Sub.Endpoint = kw.SubAddr
		cli := api.NewClient(cf)

		pubMsg := fmt.Sprintf("gk smoke test msg: [%s]", time.Now())

		err = cli.Pub("", []byte(pubMsg), api.PubOption{
			Topic: topic,
			Ver:   ver,
		})
		swallow(err)

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
				return fmt.Errorf("unexpected sub msg: %s", string(subMsg))
			}

			return api.ErrSubStop
		})
		swallow(err)

		this.Ui.Info(fmt.Sprintf("ok for %+v", kw))

		// wait for server cleanup the sub conn
		time.Sleep(time.Second)

		// 1. 查询某个pubsub topic的partition数量
		// 2. 查看pubsub系统某个topic的生产、消费状态
		// 3. pub
		// 4. sub
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

    List/Config online kateway instances

Options:

    -z zone

    -i
      Install kateway guide

    -ver
      Display kateway version only

	-zk
	  Display kateway zk config nodes and exit

    -checkup
      Checkup for online kateway instances

    -visualog access log filename
      Visualize the kateway access log with Logstalgia
      You must install Logstalgia beforehand

    -id kateway id
      Execute on a single kateway instance. By default, apply on all

    -l
      Use a long listing format

    -cf
      Enter config mode

    -reset metrics name
      Reset kateway metric counter by name

    -option <debug|gzip|hh|hhflush|jobshardid|accesslog|punish|500backoff|loglevel|dryrun|auditpub|refreshdb|auditsub|standbysub|unregroup|nometrics|ratelimit|maxreq>=<true|false|val>
      Set kateway options value
      e,g.
      dryrun=<appid.topic.ver|clear>
      refreshdb=true
      punish=3s
      500backoff=2s
      maxreq=1000
      loglevel=<info|debug|trace|warn|alarm|error>

`, this.Cmd)
	return strings.TrimSpace(help)
}
