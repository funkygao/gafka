package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/go-ozzo/ozzo-dbx"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ryanuber/columnize"
)

type Verify struct {
	Ui  cli.Ui
	Cmd string

	zone    string
	cluster string

	mode string
}

func (this *Verify) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("verify", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.mode, "mode", "p", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	lines := make([]string, 0)
	this.Ui.Output(columnize.SimpleFormat(lines))

	return
}

func (this *Verify) loadFromDb() {
	db, err := dbx.Open("mysql", "user:pass@/example")
	swallow(err)

	// create a new query
	q := db.NewQuery("SELECT id, name FROM users LIMIT 10")

	// fetch all rows into a struct array
	var users []struct {
		ID, Name string
	}
	q.All(&users)
}

func (*Verify) Synopsis() string {
	return "Verify pubsub clients synced with lagacy kafka"
}

func (this *Verify) Help() string {
	help := fmt.Sprintf(`
Usage: %s verify [options]

    Verify pubsub clients synced with lagacy kafka

Options:

    -z zone
      Default %s

    -c cluster

    -mode <p|s>


`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
