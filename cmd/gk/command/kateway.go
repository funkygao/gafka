package command

import (
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

	"github.com/funkygao/gafka/cmd/kateway/api"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
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
	resetCounter string
	listClients  bool
	checkup      bool
}

func (this *Kateway) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("kateway", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.configMode, "cf", false, "")
	cmdFlags.StringVar(&this.id, "id", "", "")
	cmdFlags.BoolVar(&this.longFmt, "l", false, "")
	cmdFlags.StringVar(&this.configOption, "option", "", "")
	cmdFlags.StringVar(&this.resetCounter, "reset", "", "")
	cmdFlags.BoolVar(&this.listClients, "clients", false, "")
	cmdFlags.StringVar(&this.logLevel, "loglevel", "", "")
	cmdFlags.BoolVar(&this.checkup, "checkup", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	if this.configMode {
		if this.logLevel != "" {
			if this.id != "" {
				kw := zkzone.KatewayInfoById(this.id)
				if kw == nil {
					panic(fmt.Sprintf("kateway %s invalid entry found in zk", this.id))
				}

				this.callKateway(kw, "PUT", fmt.Sprintf("log/%s", this.logLevel))
			} else {
				// apply on all kateways
				kws, _ := zkzone.KatewayInfos()
				for _, kw := range kws {
					this.callKateway(kw, "PUT", fmt.Sprintf("log/%s", this.logLevel))
				}
			}
		}

		if this.resetCounter != "" {
			if this.id != "" {
				kw := zkzone.KatewayInfoById(this.id)
				if kw == nil {
					panic(fmt.Sprintf("kateway %d invalid entry found in zk", this.id))
				}

				this.callKateway(kw, "DELETE", fmt.Sprintf("counter/%s", this.resetCounter))
			} else {
				// apply on all kateways
				kws, _ := zkzone.KatewayInfos()
				for _, kw := range kws {
					this.callKateway(kw, "DELETE", fmt.Sprintf("counter/%s", this.resetCounter))
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

				this.callKateway(kw, "PUT", fmt.Sprintf("options/%s/%s", k, v))
			} else {
				// apply on all kateways
				kws, _ := zkzone.KatewayInfos()
				for _, kw := range kws {
					this.callKateway(kw, "PUT", fmt.Sprintf("options/%s/%s", k, v))
				}
			}
		}

		return
	}

	if this.checkup {
		this.runCheckup(zkzone)
		return
	}

	// display mode
	mysqlDsn, err := zkzone.KatewayMysqlDsn()
	if err != nil {
		this.Ui.Error(err.Error())
		this.Ui.Warn(fmt.Sprintf("kateway[%s] mysql DSN not set on zk yet", this.zone))
		this.Ui.Output("e,g.")
		this.Ui.Output(fmt.Sprintf("%s pubsub:pubsub@tcp(10.77.135.217:10010)/pubsub?charset=utf8&timeout=10s",
			zk.KatewayMysqlPath))
		return 1
	}
	this.Ui.Output(fmt.Sprintf("mysql: %s", color.Cyan(mysqlDsn)))

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

		this.Ui.Info(fmt.Sprintf("id:%-2s host:%s cpu:%-2s up:%s",
			kw.Id, kw.Host, kw.Cpu,
			gofmt.PrettySince(kw.Ctime)))
		this.Ui.Output(fmt.Sprintf("    ver: %s\n    build: %s\n    log: %s\n    pub: %s\n    sub: %s\n    man: %s\n    dbg: %s",
			kw.Ver,
			kw.Build,
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
				this.Ui.Output("    " + client)
			}

			this.Ui.Output("    sub clients:")
			subClients := clients["sub"]
			sort.Strings(subClients)
			for _, client := range subClients {
				this.Ui.Output("    " + client)
			}
		}
	}

	if len(kateways) > 0 {
		this.Ui.Output("")
		this.Ui.Output(fmt.Sprintf("max id:%s", kateways[len(kateways)-1].Id))
	}

	return
}

func (this *Kateway) getClientsInfo(url string) map[string][]string {
	url = fmt.Sprintf("http://%s/clients", url)
	body, err := this.callHttp(url, "GET")
	if err != nil {
		return nil
	}

	var v map[string][]string
	json.Unmarshal(body, &v)
	return v
}

func (this Kateway) getKatewayStatus(url string) string {
	url = fmt.Sprintf("http://%s/status", url)
	body, err := this.callHttp(url, "GET")
	if err != nil {
		return err.Error()
	}

	return string(body)
}

func (this *Kateway) getKatewayLogLevel(url string) string {
	url = fmt.Sprintf("http://%s/status", url)
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
	switch this.zone {
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

		cf := api.DefaultConfig()
		cf.AppId = myApp
		cf.Debug = false
		cf.Secret = secret
		cli := api.NewClient(myApp, cf)
		cli.Connect(fmt.Sprintf("http://%s", kw.PubAddr))
		msgId := rand.Int()
		msg := fmt.Sprintf("smoke %d", msgId)
		this.Ui.Output(fmt.Sprintf("Pub: %s", msg))
		err := cli.Publish(topic, ver, "", []byte(msg))
		swallow(err)

		cli.Connect(fmt.Sprintf("http://%s", kw.SubAddr))
		cli.Subscribe(hisApp, topic, ver, "__smoketestonly__", func(statusCode int, msg []byte) error {
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

func (*Kateway) Synopsis() string {
	return "List/Config online kateway instances"
}

func (this *Kateway) Help() string {
	help := fmt.Sprintf(`
Usage: %s kateway -z zone [options]

    List/Config online kateway instances

Options:

    -checkup
      Checkup for online kateway instances

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
    
    -option <debug|clients|nometrics|ratelimit>=<true|false>
      Set kateway options value

`, this.Cmd)
	return strings.TrimSpace(help)
}
