package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/pkg/browser"
)

type Jsf struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Jsf) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("jsf", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing interface name")
		return 2
	}

	interfaceNames := strings.Split(args[len(args)-1], ",")
	this.showReport(interfaceNames)

	return
}

func (this *Jsf) showReport(interfaceNames []string) {
	for i, interfaceName := range interfaceNames {
		if strings.TrimSpace(interfaceName) == "" {
			continue
		}

		url := fmt.Sprintf("http://old.jsf.jd.com/monitor/es/monitor_distribute?iface=%s&method=%s", interfaceName, "")
		this.Ui.Outputf("%2d %s", i, interfaceName)
		browser.OpenURL(url)
		if i > 20 {
			break
		}
	}

}

func (*Jsf) Synopsis() string {
	return "Show JSF call report of an interface"
}

func (this *Jsf) Help() string {
	help := fmt.Sprintf(`
Usage: %s jsf interface

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
