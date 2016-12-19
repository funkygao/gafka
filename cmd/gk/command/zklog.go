package command

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/zkclient/zookeeper"
)

type ZkLog struct {
	Ui  cli.Ui
	Cmd string

	filename string
	limit    int
}

func (this *ZkLog) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("zklog", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.IntVar(&this.limit, "n", -1, "")
	cmdFlags.StringVar(&this.filename, "f", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-f").
		invalid(args) {
		return 2
	}

	this.readTransactionLog(this.filename)

	return
}

func (this *ZkLog) readTransactionLog(filename string) {
	f, err := os.Open(filename) // readonly
	swallow(err)
	defer f.Close()

	fhdr := &zookeeper.FileHeader{}
	fhdr.Deserialize(f)

	// crcValue:64
	for {

	}
}

func (*ZkLog) Synopsis() string {
	return "Parse zookeeper transaction/snapshot log file"
}

func (this *ZkLog) Help() string {
	help := fmt.Sprintf(`
Usage: %s zklog [options]

    %s

    -f segment file name   

    -n limit
      Default unlimited.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
