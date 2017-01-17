package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

// chain of dependencies, performance metrics, prioritization
type Capacity struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Capacity) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("capacity", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (this *Capacity) Synopsis() string {
	return "Intent-based capacity planning generate resource allocation plan"
}

func (this *Capacity) Help() string {
	help := fmt.Sprintf(`
Usage: %s capacity [options]

    %s

Options:


`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
