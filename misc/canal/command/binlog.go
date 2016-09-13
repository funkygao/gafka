package command

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type Binlog struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Binlog) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("binlog", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	syncer := replication.NewBinlogSyncer(1001, "mysql")
	syncer.RegisterSlave("localhost", 3306, "root", "")
	binlogFile := "mysql-bin.000080"
	binlogPos := uint32(1)
	stream, err := syncer.StartSync(mysql.Position{binlogFile, binlogPos})
	if err != nil {
		panic(err)
	}
	for {
		evt, err := stream.GetEvent()
		if err != nil {
			panic(err)
		}

		evt.Dump(os.Stdout)
	}

	return
}

func (*Binlog) Synopsis() string {
	return "Sync binlog from MySQL master to elsewhere"
}

func (this *Binlog) Help() string {
	help := fmt.Sprintf(`
Usage: %s binlog [options]

    Sync binlog from MySQL master to elsewhere

Options:


`, this.Cmd)
	return strings.TrimSpace(help)
}
