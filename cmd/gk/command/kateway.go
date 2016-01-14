package command

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"

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

	zone          string
	showConsumers bool
}

func (this *Kateway) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("kateway", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.showConsumers, "consumers", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
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

		this.Ui.Info(fmt.Sprintf("%s id:%-2s up:%s", info["host"], instance,
			gofmt.PrettySince(zk.ZkTimestamp(stat.Ctime).Time())))
		this.Ui.Output(fmt.Sprintf("    ver: %s\n    build: %s\n    pub: %s\n    sub: %s\n    man: %s\n    dbg: %s",
			info["ver"],
			info["build"],
			info["pub"],
			info["sub"],
			info["man"],
			info["debug"],
		))

		if this.showConsumers {
			// FIXME
			this.showOnlineConsumers("http://" + info["man"] + "/consumers")
		}

	}

	return
}

func (this *Kateway) showOnlineConsumers(url string) {
	response, err := http.Get(url)
	swallow(err)

	b, err := ioutil.ReadAll(response.Body)
	swallow(err)

	response.Body.Close()

	consumers := make(map[string]string)
	json.Unmarshal(b, &consumers)

	for addr, info := range consumers {
		this.Ui.Output(fmt.Sprintf("    consumers: %s  %+v", addr, info))
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

    -consumers
      Display online consumers of kateway

`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
