package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/pkg/browser"
)

type Ump struct {
	Ui  cli.Ui
	Cmd string

	appName     string
	umpRegistry map[string]string // appName:appId
}

func (this *Ump) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("ump", flag.ContinueOnError)
	cmdFlags.StringVar(&this.appName, "app", "", "")
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

func (*Ump) Synopsis() string {
	return "Display UMP page for a specified key"
}

func (this *Ump) Help() string {
	help := fmt.Sprintf(`
Usage: %s ump key

    %s

e,g. jp ump -app eclp-goods com.jd.eclp.master.goods.service.impl.GoodsServiceImpl.getGoods

Options:

    -app appName
      e,g. eclp-goods
      Valid apps: eclp-goods, eclp-master

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
