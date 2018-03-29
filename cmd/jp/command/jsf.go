package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/pkg/browser"
)

type Jsf struct {
	Ui  cli.Ui
	Cmd string

	appName string
	umpKey  string
}

func (this *Jsf) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("jsf", flag.ContinueOnError)
	cmdFlags.StringVar(&this.appName, "app", "", "")
	cmdFlags.StringVar(&this.umpKey, "k", "", "`")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	var interfaceNames []string
	if this.appName != "" {
		b, err := ioutil.ReadFile(jsfFile(this.appName))
		swallow(err)
		interfaceNames = strings.Split(string(b), ",")
	} else if this.umpKey != "" {
		this.showMethodReport(this.umpKey)
	} else if len(args) == 0 {
		this.Ui.Error("missing interface name")
		return 2
	} else {
		interfaceNames = strings.Split(args[len(args)-1], ",")
	}

	if len(interfaceNames) > 10 {
		// FIXME 10 is magic
		interfaceNames = interfaceNames[:10]
	}
	this.showReport(interfaceNames)

	return
}

func (this *Jsf) showMethodReport(umpKey string) {
	interfaceName, methodName, err := umpkey2interface(umpKey)
	swallow(err)
	url := fmt.Sprintf("http://old.jsf.jd.com/monitor/es/monitor_distribute?iface=%s&method=%s", interfaceName, methodName)
	browser.OpenURL(url)
}

func (this *Jsf) showReport(interfaceNames []string) {
	for i, interfaceName := range interfaceNames {
		if strings.TrimSpace(interfaceName) == "" {
			continue
		}

		url := fmt.Sprintf("http://old.jsf.jd.com/monitor/es/monitor_distribute?iface=%s&method=%s", interfaceName, "")
		this.Ui.Outputf("%2d %s", i+1, interfaceName)
		browser.OpenURL(url)
	}

}

func (*Jsf) Synopsis() string {
	return "Show JSF call report of an interface"
}

func (this *Jsf) Help() string {
	help := fmt.Sprintf(`
Usage: %s jsf interface

    %s

Options:    

    -app appName
      Load interfaces from %s

    -k ump key
      Implementation method name.
      e,g. com.jd.eclp.master.goods.service.impl.GoodsServiceImpl.getGoods

`, this.Cmd, this.Synopsis(), jsfFile(this.appName))
	return strings.TrimSpace(help)
}
