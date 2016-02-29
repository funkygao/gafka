package command

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

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
	resetCounter string
	listClients  bool
}

func (this *Kateway) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("kateway", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.configMode, "cf", false, "")
	cmdFlags.StringVar(&this.id, "id", "", "")
	cmdFlags.StringVar(&this.configOption, "option", "", "")
	cmdFlags.BoolVar(&this.listClients, "clients", false, "")
	cmdFlags.StringVar(&this.logLevel, "loglevel", "info", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	if this.configMode {
		switch {
		case this.logLevel != "":
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

		case this.resetCounter != "":
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

		case this.configOption != "":
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

func (*Kateway) Synopsis() string {
	return "List/Config online kateway instances"
}

func (this *Kateway) Help() string {
	help := fmt.Sprintf(`
Usage: %s kateway -z zone [options]

    List/Config online kateway instances

Options:

    -id kateway id
      Execute on a single kateway instance. By default, apply on all

    -clients
      List online pub/sub clients
   
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
