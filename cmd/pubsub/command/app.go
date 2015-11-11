package command

import (
	"flag"
	"strings"

	"github.com/funkygao/gocli"
)

type App struct {
	Ui cli.Ui
}

func (this *App) Run(args []string) (exitCode int) {
	var (
		id string
	)
	cmdFlags := flag.NewFlagSet("app", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "init", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if id == "" {
		this.Ui.Output("please provide app id")
		this.Ui.Output(this.Help())
		return 2
	}

	// init
	NewZk(DefaultConfig(id, zkAddr)).Init()
	this.Ui.Output("init success")

	return

}

func (*App) Synopsis() string {
	return "Application initialization"
}

func (*App) Help() string {
	help := `
Usage: pubsub app -init appId

	Application initialization
`
	return strings.TrimSpace(help)
}
