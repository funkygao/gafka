package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Controller struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Controller) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("controller", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.Ui.Output(`
Kafka controller 
    contains:
        PartitionStateMachine
        ReplicaStateMachine

    leader elction:
        /controller

    subscribeDataChanges:
        /controller_epoch
        /admin/reassign_partitions
        /admin/preferred_replica_election
        /brokers/topics/$topic
        /brokers/topics/$topic/partitions/$partition/state

    subscribeChildChanges:
        /brokers/topics   
        /admin/delete_topics
        /brokers/ids
		`)

	return
}

func (*Controller) Synopsis() string {
	return "Explains kafka controller mechanism"
}

func (this *Controller) Help() string {
	help := fmt.Sprintf(`
Usage: %s controller [options]

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
