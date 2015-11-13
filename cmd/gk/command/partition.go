package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Partition struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Partition) Run(args []string) (exitCode int) {
	// ./bin/kafka-topics.sh --zookeeper localhost:2181/kafka --alter --topic foobar --partitions 5
	return

}

func (*Partition) Synopsis() string {
	return "Add partition num to a topic TODO"
}

func (this *Partition) Help() string {
	help := fmt.Sprintf(`
Usage: %s partition -z zone -c cluster -t topic [options]
`, this.Cmd)
	return strings.TrimSpace(help)
}
