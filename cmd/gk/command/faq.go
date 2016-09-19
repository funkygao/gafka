package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Faq struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Faq) Run(args []string) (exitCode int) {
	content := fmt.Sprintf(`
What is zone?
    zone is named after AWS zone with the same sematics. 
    It is a group of kafka clusters that share the same Zookeeper ensemble.
    They can be located in different IDC or in the same IDC with different environments.
    %s will automatically install zones in your $HOME/.gafka.cf

What is cluster?
    cluster is a kafka cluster: a group of kafka brokers that share the
    same zookeeper.connect and different broker.id

How can I get help?
    %s <command> -h

`, this.Cmd, this.Cmd)

	this.Ui.Output(strings.TrimSpace(content))
	return
}

func (*Faq) Synopsis() string {
	return "FAQ"
}

func (this *Faq) Help() string {
	help := fmt.Sprintf(`
Usage: %s ?

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
