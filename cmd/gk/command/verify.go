package command

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/go-ozzo/ozzo-dbx"
	_ "github.com/go-sql-driver/mysql"
	"github.com/olekukonko/tablewriter"
)

type TopicInfo struct {
	AppId          string `db:"AppId"`
	TopicName      string `db:"TopicName"`
	TopicIntro     string `db:"TopicIntro"`
	KafkaTopicName string `db:"KafkaTopicName"`
}

type Verify struct {
	Ui  cli.Ui
	Cmd string

	zone    string
	cluster string

	mode     string
	mysqlDsn string

	topics []TopicInfo
}

func (this *Verify) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("verify", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "bigtopic", "")
	cmdFlags.StringVar(&this.mode, "mode", "p", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z").
		invalid(args) {
		return 2
	}

	ensureZoneValid(this.zone)

	mysqlDsns := map[string]string{
		"prod": "user_pubsub:p0nI7mEL6OLW@tcp(m3342.wdds.mysqldb.com:3342)/pubsub?charset=utf8&timeout=10s",
		"sit":  "pubsub:pubsub@tcp(10.209.44.12:10043)/pubsub?charset=utf8&timeout=10s",
		"test": "pubsub:pubsub@tcp(10.209.44.14:10044)/pubsub?charset=utf8&timeout=10s",
	}

	this.mysqlDsn = mysqlDsns[this.zone]

	switch this.mode {
	case "p":
		this.verifyPub()

	case "s":
		this.verifySub()
	}

	return
}

func (this *Verify) verifyPub() {
	this.loadFromManager()

	table := tablewriter.NewWriter(os.Stdout)
	for _, t := range this.topics {
		table.Append([]string{t.AppId, t.TopicName, t.TopicIntro, t.KafkaTopicName})
	}
	table.SetHeader([]string{"Id", "Topic", "Desc", "Kafka"})
	table.Render() // Send output
}

func (this *Verify) verifySub() {

}

func (this *Verify) loadFromManager() {
	db, err := dbx.Open("mysql", this.mysqlDsn)
	swallow(err)

	// TODO fetch from topics_version
	q := db.NewQuery("SELECT AppId, TopicName, TopicIntro, KafkaTopicName FROM topics WHERE Status = 1 ORDER BY CategoryId, TopicName")
	swallow(q.All(&this.topics))
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
