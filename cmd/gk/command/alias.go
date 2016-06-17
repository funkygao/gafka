package command

import (
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
	aliases := ctx.AliasesWithValue()
	sortedNames := make([]string, 0, len(aliases))
	for name, _ := range aliases {
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
	return "Display all aliases"
}

func (this *Alias) Help() string {
	help := fmt.Sprintf(`
Usage: %s alias

    Display all aliases

`, this.Cmd)
	return strings.TrimSpace(help)
}
