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
	"sort"
	"strings"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/golib/pipestream"
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
	listClients  bool
	visualLog    string
	checkup      bool
	versionOnly  bool
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
	cmdFlags.BoolVar(&this.listClients, "clients", false, "")
	cmdFlags.StringVar(&this.logLevel, "loglevel", "", "")
	cmdFlags.StringVar(&this.visualLog, "visualog", "", "")
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
					panic(fmt.Sprintf("kateway %d invalid entry found in zk", this.id))
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
			k, v := parts[0], parts[1]
			if this.id != "" {
				kw := zkzone.KatewayInfoById(this.id)
				if kw == nil {
					panic(fmt.Sprintf("kateway %d invalid entry found in zk", this.id))
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

	// display mode
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

			disques, err := zkzone.KatewayDisqueAddrs()
			if err != nil {
				this.Ui.Warn(fmt.Sprintf("kateway[%s] disque not set on zk:%s", zkzone.Name(),
					zk.KatewayDisquePath))
			} else {
				this.Ui.Output(fmt.Sprintf("zone[%s]     disque: %+v",
					color.Cyan(zkzone.Name()), disques))
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

			if this.versionOnly {
				this.Ui.Info(fmt.Sprintf("%6s id:%-2s host:%s ver:%s build:%s/%s cpu:%-2s up:%s",
					zkzone.Name(),
					kw.Id, kw.Host,
					kw.Ver, kw.Build, kw.BuiltAt,
					kw.Cpu,
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
				this.getKatewayLogLevel(kw.ManAddr),
				kw.PubAddr,
				kw.SubAddr,
				kw.ManAddr,
				kw.DebugAddr,
			))

			if this.longFmt {
				this.Ui.Output("    full status:")
				this.Ui.Output(this.getKatewayStatus(kw.ManAddr))
			}

			if this.listClients {
				clients := this.getClientsInfo(kw.ManAddr)
				this.Ui.Output("    pub clients:")
				pubClients := clients["pub"]
				sort.Strings(pubClients)
				for _, client := range pubClients {
					// pub client in blue
					this.Ui.Output(color.Blue("      %s", client))
				}

				this.Ui.Output("    sub clients:")
				subClients := clients["sub"]
				sort.Strings(subClients)
				for _, client := range subClients {
					// sub client in
					this.Ui.Output(color.Yellow("      %s", client))
				}
			}
		}

	})

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
        broker_list => "10.209.18.15:11003,10.209.18.16:11003"
        topic_id => "pubsub_log"
        topic_metadata_refresh_interval_ms => 600000
    }
}
		`))
	this.Ui.Info("chkconfig --add logstash")
	this.Ui.Info("/etc/init.d/logstash start")
}

func (this *Kateway) getClientsInfo(url string) map[string][]string {
	url = fmt.Sprintf("http://%s/v1/clients", url)
	body, err := this.callHttp(url, "GET")
	if err != nil {
		return nil
	}

	var v map[string][]string
	json.Unmarshal(body, &v)
	return v
}

func (this Kateway) getKatewayStatus(url string) string {
	url = fmt.Sprintf("http://%s/v1/status", url)
	body, err := this.callHttp(url, "GET")
	if err != nil {
		return err.Error()
	}

	return string(body)
}

func (this *Kateway) getKatewayLogLevel(url string) string {
	url = fmt.Sprintf("http://%s/v1/status", url)
	body, err := this.callHttp(url, "GET")
	if err != nil {
		return err.Error()
	}

	var v map[string]interface{}
	json.Unmarshal(body, &v)
	return v["loglevel"].(string)
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
	_, err = this.callHttp(url, method)
	return
}

func (this *Kateway) runCheckup(zkzone *zk.ZkZone) {
	var (
		myApp  string
		hisApp string
		secret string
		ver    string = "v1"
		topic  string = "smoketestonly"
	)
	switch zkzone.Name() {
	case "sit":
		myApp = "35"
		hisApp = "35"
		secret = "04dd44d8dad048e6a18ffd153eb8f642"

	case "prod":
		myApp = "30"
		hisApp = "30"
		secret = "32f02594f55743eeb1efcf75db6dd8a0"
	}

	rand.Seed(time.Now().UTC().UnixNano())

	kws, err := zkzone.KatewayInfos()
	swallow(err)
	for _, kw := range kws {
		if this.id != "" && kw.Id != this.id {
			continue
		}

		cf := api.DefaultConfig(myApp, secret)
		cf.Pub.Endpoint = kw.PubAddr
		cf.Sub.Endpoint = kw.SubAddr
		cli := api.NewClient(cf)
		msgId := rand.Int()
		msg := fmt.Sprintf("smoke %d", msgId)
		this.Ui.Output(fmt.Sprintf("Pub: %s", msg))

		err := cli.Pub("", []byte(msg), api.PubOption{
			Topic: topic,
			Ver:   ver,
		})
		swallow(err)

		cli.Sub(api.SubOption{
			AppId: hisApp,
			Topic: topic,
			Ver:   ver,
			Group: "__smoketestonly__",
		}, func(statusCode int, msg []byte) error {
			if statusCode == http.StatusNoContent {
				this.Ui.Output("no content, sub again")
				return nil
			}

			this.Ui.Output(fmt.Sprintf("Sub: %s, http:%s", string(msg),
				http.StatusText(statusCode)))

			return api.ErrSubStop
		})

		this.Ui.Info(fmt.Sprintf("curl -H'Appid: %s' -H'Subkey: %s' -i http://%s/status/%s/%s/%s",
			myApp, secret, kw.SubAddr, hisApp, topic, ver))

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
Usage: %s kateway -z zone [options]

    List/Config online kateway instances

Options:

    -i
      Install kateway guide

    -ver
      Display kateway version only

    -checkup
      Checkup for online kateway instances

    -visualog access log filename
      Visualize the kateway access log with Logstalgia
      You must install Logstalgia beforehand

    -id kateway id
      Execute on a single kateway instance. By default, apply on all

    -clients
      List online pub/sub clients

    -l
      Use a long listing format
   
    -cf
      Enter config mode

    -reset metrics name
      Reset kateway metric counter by name

    -loglevel <info|debug|trace|warn|alarm|error>
      Set kateway log level
    
    -option <debug|clients|gzip|accesslog|standbysub|unregroup|nometrics|ratelimit|maxreq>=<true|false|int>
      Set kateway options value

`, this.Cmd)
	return strings.TrimSpace(help)
}
