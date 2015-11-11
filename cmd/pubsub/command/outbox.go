package command

import (
	"flag"
	"strings"

	"github.com/funkygao/gocli"
)

type Outbox struct {
	Ui cli.Ui
}

func (this *Outbox) Run(args []string) (exitCode int) {
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

func (*Outbox) Synopsis() string {
	return "Manipulate my outboxes"
}

func (*Outbox) Help() string {
	help := `
Usage: pubsub outbox -id appId [options]

	Manipulate my outboxes

Options:
  
  -list

  -add topic
`
	return strings.TrimSpace(help)
}
