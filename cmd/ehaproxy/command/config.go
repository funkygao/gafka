package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Config struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Config) Run(args []string) (exitCode int) {

	return
}

func (this *Config) Synopsis() string {
	return fmt.Sprintf("Display %s active configuration", this.Cmd)
}

func (this *Config) Help() string {
	help := fmt.Sprintf(`
Usage: %s config

    Display %s active configuration

`, this.Cmd, this.Cmd)
	return strings.TrimSpace(help)
}
