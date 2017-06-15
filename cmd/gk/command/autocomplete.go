package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Autocomplete struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Autocomplete) Run(args []string) (exitCode int) {
	var target string
	cmdFlags := flag.NewFlagSet("autocomplete", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&target, "for", "gk", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if target != "gk" && target != "zk" && target != "dbc" && target != "es" {
		this.Ui.Output(this.Help())
		return 2
	}

	src := fmt.Sprintf("template/bash_autocomplete.%s", target)
	dst := fmt.Sprintf("/etc/bash_completion.d/%s", target)
	writeFileFromTemplate(src, dst, 0644, nil, nil)

	this.Ui.Infof("autocompletion script for %s installed, next:", target)
	this.Ui.Warn("yum install -y bash-completion")
	this.Ui.Warnf("source %s", dst)

	return
}

func (this *Autocomplete) Synopsis() string {
	return "Setup bash auto completion"
}

func (this *Autocomplete) Help() string {
	help := fmt.Sprintf(`
Usage: %s autocomplete [options]

    %s

Options:

    -for <gk|zk|dbc|es>
      Defaults gk

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
