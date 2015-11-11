package command

import (
	"flag"
	"strings"

	"github.com/funkygao/gocli"
)

type Inbox struct {
	Ui cli.Ui
}

func (this *Inbox) Run(args []string) (exitCode int) {
	var (
		id   string
		list bool
		add  string
	)
	cmdFlags := flag.NewFlagSet("bind", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "id", "", "")
	cmdFlags.StringVar(&add, "add", "", "")
	cmdFlags.BoolVar(&list, "list", true, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if id == "" {
		this.Ui.Output("-id is required")
		this.Ui.Output(this.Help())
		return 2
	}

	return

}

func (*Inbox) Synopsis() string {
	return "Manipulate my inboxes"
}

func (*Inbox) Help() string {
	help := `
Usage: pubsub inbox -id appId [options]

	Manipulate my inboxes

Options:
  
  -list

  -add topic

`
	return strings.TrimSpace(help)
}
