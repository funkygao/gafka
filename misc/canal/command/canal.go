package command

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/funkygao/gocli"
	"github.com/siddontang/go-mysql/canal"
)

type Canal struct {
	Ui  cli.Ui
	Cmd string
}

type myRowsEventHandler struct {
}

func (h *myRowsEventHandler) Do(e *canal.RowsEvent) error {
	fmt.Println(*e)
	return nil
}

func (h *myRowsEventHandler) String() string {
	return "myRowsEventHandler"
}

func (this *Canal) Run(args []string) (exitCode int) {
	cfg := canal.NewDefaultConfig()

	cmdFlags := flag.NewFlagSet("canal", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&cfg.User, "user", "", "")
	cmdFlags.StringVar(&cfg.Password, "pass", "", "")
	cmdFlags.StringVar(&cfg.Addr, "dsn", "", "")
	cmdFlags.StringVar(&cfg.Dump.TableDB, "db", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	cfg.Dump.Tables = []string{"logs"}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		this.Ui.Error(err.Error())
		return
	}
	defer c.Close()

	c.RegRowsEventHandler(&myRowsEventHandler{})

	// Start canal
	c.Start()

	time.Sleep(time.Hour)

	return
}

func (*Canal) Synopsis() string {
	return "Sync binlog from MySQL master to elsewhere"
}

func (this *Canal) Help() string {
	help := fmt.Sprintf(`
Usage: %s canal [options]

    %s

Options:

    -user db user

    -pass db password

    -dsn db dsn

    -db db name

    -table table name


`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
