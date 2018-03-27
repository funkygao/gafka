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

	loadFile bool
}

func (this *Jsf) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("jsf", flag.ContinueOnError)
	cmdFlags.BoolVar(&this.loadFile, "l", false, "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	var interfaceNames []string
	if this.loadFile {
		b, err := ioutil.ReadFile(jsfFile())
		swallow(err)
		interfaceNames = strings.Split(string(b), ",")
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

    -l
      Load interfaces from %s

`, this.Cmd, this.Synopsis(), jsfFile())
	return strings.TrimSpace(help)
}
