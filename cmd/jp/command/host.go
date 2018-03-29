package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/pkg/browser"
)

type Host struct {
	Ui  cli.Ui
	Cmd string

	appName string
}

func (this *Host) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("host", flag.ContinueOnError)
	cmdFlags.StringVar(&this.appName, "app", "eclp_goods", "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.appName == "" {
		this.Ui.Error("appName required")
		return 2
	}

	this.appName = strings.Replace(this.appName, "-", "_", -1)
	this.listServers(this.appName)

	return
}

func (*Host) listServers(appName string) {
	url := fmt.Sprintf("http://logbook.jd.com/data/manage/app_detail/%s", appName)
	browser.OpenURL(url)
}

func (*Host) Synopsis() string {
	return "List servers of an application in production environment"
}

func (this *Host) Help() string {
	help := fmt.Sprintf(`
Usage: %s host

    %s

Options:

    -app appName

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
