package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/ryanuber/columnize"
)

type Alias struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Alias) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("alias", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	aliases := ctx.AliasesWithValue()
	sortedNames := make([]string, 0, len(aliases))
	for name := range aliases {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)

	lines := make([]string, 0, len(aliases)+1)
	header := "Alias|Command"
	lines = append(lines, header)
	for _, name := range sortedNames {
		lines = append(lines, fmt.Sprintf("%s|%s", name, aliases[name]))
	}

	fmt.Println(columnize.SimpleFormat(lines))

	return
}

func (*Alias) Synopsis() string {
	return "Display all aliases defined in $HOME/.gafka.cf"
}

func (this *Alias) Help() string {
	help := fmt.Sprintf(`
Usage: %s alias

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
