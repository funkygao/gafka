package command

import (
	"fmt"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
)

type Merge struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Merge) Run(args []string) (exitCode int) {
	if len(args) == 0 {
		this.Ui.Error("missing path")
		this.Ui.Output(this.Help())
		return 2
	}

	return
}

func (this *Merge) render(path string) {
	files, _ := ioutil.ReadDir(path)
	m := make(map[string]int64)
	for _, f := range files {
		if f.Size() < 1 {
			continue
		}

		segment := f.Name()[:5]
		m[segment] += f.Size()
	}
	sortedNames := make([]string, 0, len(m))
	for s := range m {
		sortedNames = append(sortedNames, s)
	}
	sort.Strings(sortedNames)

	for _, segment := range sortedNames {
		size := m[segment]
		this.Ui.Outputf("%10s %s", segment, gofmt.ByteSize(size))
	}

}

func (*Merge) Synopsis() string {
	return "Visualize segments merge process"
}

func (this *Merge) Help() string {
	help := fmt.Sprintf(`
Usage: %s merge <path>

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
