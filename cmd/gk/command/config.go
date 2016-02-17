package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Config struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Config) Run(args []string) (exitCode int) {
	var (
		genratedMode bool
		showAliases  bool
	)
	cmdFlags := flag.NewFlagSet("config", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&genratedMode, "gen", false, "")
	cmdFlags.BoolVar(&showAliases, "alias", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if genratedMode {
		this.Ui.Output(strings.TrimSpace(ctx.DefaultConfig))
		return
	}

	if showAliases {
		this.Ui.Output("Active aliases:")
		for _, cmd := range ctx.Aliases() {
			alias, _ := ctx.Alias(cmd)
			this.Ui.Output(fmt.Sprintf("%10s = %s", cmd, alias))
		}
		return
	}

	// display $HOME/.gafka.cf
	usr, err := user.Current()
	swallow(err)
	b, err := ioutil.ReadFile(filepath.Join(usr.HomeDir, ".gafka.cf"))
	swallow(err)
	this.Ui.Info("current config file contents")
	this.Ui.Output(string(b))

	return
}

func (this *Config) Synopsis() string {
	return fmt.Sprintf("Display %s config file contents", this.Cmd)
}

func (this *Config) Help() string {
	help := fmt.Sprintf(`
Usage: %s config [options]

    Display %s config file contents

Options:

    -gen
      Display default config contents on console.

    -alias
      Display active aliases.

`, this.Cmd, this.Cmd)
	return strings.TrimSpace(help)
}
