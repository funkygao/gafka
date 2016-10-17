package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Doc struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Doc) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("doc", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.Ui.Output(`
Each record consists of a key, a value, and a timestamp.
The message format in 0.10.0 includes a new timestamp field and uses relative offsets for compressed messages.
Configuration parameter replica.lag.max.messages was removed in 0.9.0.0. Partition leaders will no longer consider the number of lagging messages when deciding which replicas are in sync. Configuration parameter replica.lag.time.max.ms now refers not just to the time passed since last fetch request from replica, but also to time since the replica last caught up. Replicas that are still fetching messages from leaders but did not catch up to the latest messages in replica.lag.time.max.ms will be considered out of sync.


Sendfile:
    FileMessageSet.writeTo
	    FileChannel.transferTo

    When broker and client version not matched, sendfile might not work
		`)

	return
}

func (*Doc) Synopsis() string {
	return "Internals of kafka"
}

func (this *Doc) Help() string {
	help := fmt.Sprintf(`
Usage: %s doc [options]

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
