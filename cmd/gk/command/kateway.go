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
	zkr "github.com/funkygao/gafka/registry/zk"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	zklib "github.com/samuel/go-zookeeper/zk"
)

type Kateway struct {
	Ui  cli.Ui
	Cmd string

	zone     string
	id       string
	logLevel string
}

func (this *Kateway) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("kateway", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.id, "id", "", "")
	cmdFlags.StringVar(&this.logLevel, "loglevel", "info", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	if validateArgs(this, this.Ui).
		on("-id", "-loglevel").
		invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))

	if this.id != "" {
		this.setupLogLevel(zkzone)
		return
	}

	mysqlDsn, err := zkzone.MysqlDsn()
	if err != nil {
		this.Ui.Error(err.Error())
		this.Ui.Warn(fmt.Sprintf("kateway[%s] mysql DSN not set on zk yet", this.zone))
		this.Ui.Output("e,g.")
		this.Ui.Output(fmt.Sprintf("%s pubsub:pubsub@tcp(10.77.135.217:10010)/pubsub?charset=utf8&timeout=10s",
			zk.KatewayMysqlPath))
		return 1
	}
	this.Ui.Output(fmt.Sprintf("mysql: %s", mysqlDsn))

	instances, _, err := zkzone.Conn().Children(zkr.Root(this.zone))
	if err != nil {
		if err == zklib.ErrNoNode {
			this.Ui.Output("no kateway running")
			return
		} else {
			swallow(err)
		}
	}
	sort.Strings(instances)

	for _, instance := range instances {
		data, stat, err := zkzone.Conn().Get(zkr.Root(this.zone) + "/" + instance)
		swallow(err)

		info := make(map[string]string)
		json.Unmarshal(data, &info)

		this.Ui.Info(fmt.Sprintf("id:%-2s host:%s cpu:%-2s up:%s",
			instance, info["host"], info["cpu"],
			gofmt.PrettySince(zk.ZkTimestamp(stat.Ctime).Time())))
		this.Ui.Output(fmt.Sprintf("    ver: %s\n    build: %s\n    log: %s\n    go: %s\n    pub: %s\n    sub: %s\n    man: %s\n    dbg: %s",
			info["ver"],
			info["build"],
			info["loglevel"],
			info["goroutines"],
			info["pub"],
			info["sub"],
			info["man"],
			info["debug"],
		))

	}

	return
}

func (this *Kateway) setupLogLevel(zkzone *zk.ZkZone) {
	data, _, err := zkzone.Conn().Get(zkr.Root(this.zone) + "/" + this.id)
	swallow(err)

	info := make(map[string]string)
	json.Unmarshal(data, &info)

	var req *http.Request
	url := fmt.Sprintf("http://%s/log/%s", info["man"], this.logLevel)
	req, err = http.NewRequest("PUT", url, nil)
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

	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}

	response.Body.Close()

	if response.StatusCode != http.StatusOK {
		this.Ui.Error(response.Status)
		this.Ui.Error(string(b))
	} else {
		this.Ui.Info("done")
	}

}

func (*Kateway) Synopsis() string {
	return "List online kateway instances"
}

func (this *Kateway) Help() string {
	help := fmt.Sprintf(`
Usage: %s kateway [options]

    List online kateway instances

Options:

    -z zone
      Default %s

    -id kateway id

    -loglevel <info|debug|trace|warn|alarm|error>
      Set kateway log level

`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
