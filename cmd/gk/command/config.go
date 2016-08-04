package command

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/funkygao/gocli"
)

type Config struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Config) Run(args []string) (exitCode int) {
	var (
		bashAutocomplete bool
	)
	cmdFlags := flag.NewFlagSet("config", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&bashAutocomplete, "auto", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if bashAutocomplete {
		writeFileFromTemplate("template/bash_autocomplete", "/etc/bash_completion.d/gk",
			0644, nil, nil)

		this.Ui.Info("next:")
		this.Ui.Warn("yum install -y bash-completion")
		this.Ui.Warn("source /etc/bash_completion.d/gk")
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

    -auto
      Install gk bash autocomplete script.  

`, this.Cmd, this.Cmd)
	return strings.TrimSpace(help)
}
