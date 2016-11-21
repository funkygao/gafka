package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type OffsetManager struct {
	Ui  cli.Ui
	Cmd string
}

func (this *OffsetManager) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("offset", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.Ui.Output(`
ByteBufferMessageSet.assignOffsets()
		
Topic: __consumer_offsets

Key: [Group, Topic, Partition]
Val: [Offset, Metadata, Timestamp]

query is always looked up in offsetCache

server.properties
    offset.storage=kafka
    #dual.commit.enabled=true
		`)

	return
}

func (*OffsetManager) Synopsis() string {
	return "How kafka store consumer group offset"
}

func (this *OffsetManager) Help() string {
	help := fmt.Sprintf(`
Usage: %s offset [options]

    %s

    -d log.dirs count

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
