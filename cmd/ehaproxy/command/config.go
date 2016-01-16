package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/funkygao/gocli"
)

type Config struct {
	Ui  cli.Ui
	Cmd string

	root string
}

func (this *Config) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("config", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.root, "p", defaultPrefix, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	b, e := ioutil.ReadFile(fmt.Sprintf("%s/%s", this.root, configFile))
	swalllow(e)

	this.Ui.Output(string(b))

	return
}

func (this *Config) Synopsis() string {
	return fmt.Sprintf("Display %s active configuration", this.Cmd)
}

func (this *Config) Help() string {
	help := fmt.Sprintf(`
Usage: %s config [options]

    Display %s active configuration

Options:

    -p prefix
      Default %s

`, this.Cmd, this.Cmd, defaultPrefix)
	return strings.TrimSpace(help)
}
