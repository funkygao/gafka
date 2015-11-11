package command

import (
	"flag"
	"strings"

	"github.com/funkygao/gocli"
)

type Pub struct {
	Ui cli.Ui
}

func (this *Pub) Run(args []string) (exitCode int) {
	var (
		id      string
		console bool
		stress  bool
		size    int
		topic   string
	)
	cmdFlags := flag.NewFlagSet("pub", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "id", "", "")
	cmdFlags.StringVar(&topic, "topic", "", "")
	cmdFlags.BoolVar(&console, "console", true, "")
	cmdFlags.BoolVar(&stress, "stress", false, "")
	cmdFlags.IntVar(&size, "size", 100, "")
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

func (*Pub) Synopsis() string {
	return "Publish to my outbox"
}

func (*Pub) Help() string {
	help := `
Usage: pubsub pub -id appId [options]

Options:

  -topic name
  
  -console

  -stress

  -size size
   	Under stress mode, specify each msg size.
`
	return strings.TrimSpace(help)
}
