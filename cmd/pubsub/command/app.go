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
		id   string
		show bool
	)
	cmdFlags := flag.NewFlagSet("app", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "init", "", "")
	cmdFlags.BoolVar(&show, "show", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return

}

func (*App) Synopsis() string {
	return "Application initialization and diagnose"
}

func (*App) Help() string {
	help := `
Usage: pubsub app [options]

	Application initialization and diagnose

Options:

  -init appId
  
  -show
`
	return strings.TrimSpace(help)
}
