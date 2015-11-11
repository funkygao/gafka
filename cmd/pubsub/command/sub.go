package command

import (
	"flag"
	"strings"

	//"github.com/Shopify/sarama"
	"github.com/funkygao/gocli"
)

type Sub struct {
	Ui cli.Ui
}

func (this *Sub) Run(args []string) (exitCode int) {
	var (
		id   string
		step int
	)
	cmdFlags := flag.NewFlagSet("sub", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&id, "id", "", "")
	cmdFlags.IntVar(&step, "step", 100, "")
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

func (*Sub) Synopsis() string {
	return "Receive messages from my inbox"
}

func (*Sub) Help() string {
	help := `
Usage: pubsub sub -id appId [options]

	Receive messages from my inbox

Options:
  
  -step n
`
	return strings.TrimSpace(help)
}
