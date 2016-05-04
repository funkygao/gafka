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

type AppInfo struct {
	AppId            string `db:"AppId"`
	ApplicationName  string `db:"ApplicationName"`
	ApplicationIntro string `db:"ApplicationIntro"`
	Cluster          string `db:"Cluster"`
	CreateBy         string `db:"CreateBy"`
	CreateTime       string `db:"CreateTime"`
	Status           string `db:"Status"`
}

type Whois struct {
	Ui  cli.Ui
	Cmd string

	zone string
	app  string

	db *dbx.DB

	appInfos []AppInfo
}

func (this *Whois) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("whois", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.app, "app", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	ensureZoneValid(this.zone)

	mysqlDsns := map[string]string{
		"prod": "user_pubsub:p0nI7mEL6OLW@tcp(m3342.wdds.mysqldb.com:3342)/pubsub?charset=utf8&timeout=10s",
		"sit":  "pubsub:pubsub@tcp(10.209.44.12:10043)/pubsub?charset=utf8&timeout=10s",
		"test": "pubsub:pubsub@tcp(10.209.44.14:10044)/pubsub?charset=utf8&timeout=10s",
	}
	this.loadFromManager(mysqlDsns[this.zone])

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Id", "Name", "Desc", "Who", "Ctime"})
	for _, ai := range this.appInfos {
		table.Append([]string{ai.AppId, ai.ApplicationName, ai.ApplicationIntro, ai.CreateBy, ai.CreateTime})
	}
	table.Render()

	return
}

func (this *Whois) loadFromManager(dsn string) {
	db, err := dbx.Open("mysql", dsn)
	swallow(err)

	this.db = db

	// TODO fetch from topics_version
	sql := "SELECT AppId,ApplicationName,ApplicationIntro,Cluster,CreateBy,CreateTime,Status FROM application ORDER BY AppId"
	if this.app != "" {
		sql += " WHERE AppId IN (" + this.app + ")"
	}
	sql += " ORDER BY AppId"
	q := db.NewQuery(sql)
	swallow(q.All(&this.appInfos))
}

func (*Whois) Synopsis() string {
	return "Lookup PubSub App Information"
}

func (this *Whois) Help() string {
	help := fmt.Sprintf(`
Usage: %s whois -z zone [options]

    Lookup PubSub App Information

Options:

    -app comma seperated appId    

`, this.Cmd)
	return strings.TrimSpace(help)
}
