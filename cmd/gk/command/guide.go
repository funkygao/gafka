package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Guide struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Guide) Run(args []string) (exitCode int) {
	guideContent := `
What can gk do?
    gk can monitor/control/deploy kafka/zookeeper/kateway

What is zone?
    zone is named after AWS zone with the same sematics. 
    It is a group of kafka clusters that share the same Zookeeper ensemble.
    They can be located in different IDC or in the same IDC with different environments.
    gk will automatically install zones in your $HOME/.gafka.cf

How can I get help?
    gk <cmd> -h

`

	this.Ui.Output(strings.TrimSpace(guideContent))
	return
}

func (*Guide) Synopsis() string {
	return "Manual of gk"
}

func (this *Guide) Help() string {
	help := fmt.Sprintf(`
Usage: %s ?

    Manual of gk

`, this.Cmd)
	return strings.TrimSpace(help)
}
