package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	//"github.com/funkygao/gorequest"
	"github.com/pkg/browser"
)

// http://open.ump.jd.com/queryMonitorData
// 存活监控
// JVM
// 业务
type Ump struct {
	Ui  cli.Ui
	Cmd string

	appName     string
	umpRegistry map[string]string // appName:appId
}

func (this *Ump) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("ump", flag.ContinueOnError)
	cmdFlags.StringVar(&this.appName, "app", "eclp_goods", "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing key")
		return 2
	}

	this.umpRegistry = map[string]string{
		"eclp_goods":  "5397",
		"eclp_master": "5390",
	}

	keys := strings.Split(args[len(args)-1], ",")
	this.showReport(keys)

	return
}

func (this *Ump) showReport(keys []string) {
	for _, k := range keys {
		url := fmt.Sprintf("http://ump.jd.com/performanceReport/initPage.action?queryMap.appName=%s&queryMap.appId=%s&queryMap.accessKey=%s&queryMap.analysisFrequency=1&queryMap.departCode=33138&queryMap.groupId=",
			this.appName, this.umpRegistry[this.appName], k)
		browser.OpenURL(url)
	}
}

// curl -i -XPOST -H"Content-Type: application/json" -H'token:xx' http://open.ump.jd.com/queryMonitorData -d'{"monitorType": "Method", "scope":"Key", "dataType":"AVG,TP90,TP99,TP999,TotalCount,SuccessCount,FailCount,AvailRate", "scopeValues":"com.jd.eclp.master.goods.service.impl.GoodsServiceImpl.getGoods", "dagaCycle":"oneMinute", "startTime":"", "endTime":""}'
func (this *Ump) fetchUmpMetrics() {
}

func (*Ump) Synopsis() string {
	return "Display UMP page for a specified key"
}

func (this *Ump) Help() string {
	help := fmt.Sprintf(`
Usage: %s ump keys

    %s

keys seperated by comma
e,g. jp ump -app eclp-goods com.jd.eclp.master.goods.service.impl.GoodsServiceImpl.getGoods

Options:

    -app appName
      Default is eclp-goods.
      Valid apps: eclp-goods, eclp-master

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
