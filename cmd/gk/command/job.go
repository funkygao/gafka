package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/funkygao/fae/config"
	"github.com/funkygao/fae/servant/mysql"
	"github.com/funkygao/gafka/cmd/kateway/job"
	jm "github.com/funkygao/gafka/cmd/kateway/job/mysql"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/ryanuber/columnize"
)

type Job struct {
	Ui  cli.Ui
	Cmd string

	mc     *mysql.MysqlCluster
	zkzone *zk.ZkZone
	due    int
}

func (this *Job) Run(args []string) (exitCode int) {
	var (
		zone  string
		appid string
	)
	cmdFlags := flag.NewFlagSet("job", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&appid, "app", "", "")
	cmdFlags.IntVar(&this.due, "d", 0, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.zkzone = zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	if appid != "" {
		this.displayAppJobs(appid)
		return
	}

	this.printJobQueueAndActors()

	return
}

// TODO diagnose all app's jobs status
func (this *Job) displayAppJobs(appid string) {
	aid := this.connectMysqlCluster(appid)
	lines := make([]string, 0)
	header := "Topic|Type|JobId|Due|Payload"
	lines = append(lines, header)

	// FIXME does not respect appid, show all now
	this.forSortedJobQueues(func(topic string) {
		table := jm.JobTable(topic)
		archiveTable := jm.HistoryTable(topic)

		sqlRealTime := fmt.Sprintf("SELECT job_id,app_id,payload,due_time FROM %s ORDER BY due_time DESC", table)
		if this.due > 0 {
			sqlRealTime = fmt.Sprintf("SELECT job_id,app_id,payload,due_time FROM %s WHERE due_time<=? ORDER BY due_time DESC",
				table, time.Now().Unix()+int64(this.due))
		}
		rows, err := this.mc.Query(jm.AppPool, table, aid, sqlRealTime)
		swallow(err)

		var item job.JobItem
		for rows.Next() {
			err = rows.Scan(&item.JobId, &item.AppId, &item.Payload, &item.DueTime)
			swallow(err)
			lines = append(lines, fmt.Sprintf("%s|RT|%d|%d|%s", topic, item.JobId, item.DueTime, item.PayloadString(50)))
		}
		rows.Close()

		sqlArchive := fmt.Sprintf("SELECT job_id,app_id,payload,due_time FROM %s ORDER BY due_time ASC LIMIT 100", archiveTable)
		rows, err = this.mc.Query(jm.AppPool, table, aid, sqlArchive)
		swallow(err)

		for rows.Next() {
			err = rows.Scan(&item.JobId, &item.AppId, &item.Payload, &item.DueTime)
			swallow(err)
			lines = append(lines, fmt.Sprintf("%s|AR|%d|%d|%s", topic, item.JobId, item.DueTime, item.PayloadString(50)))
		}
		rows.Close()
	})

	if len(lines) > 1 {
		this.Ui.Output(columnize.SimpleFormat(lines))
	}
}

func (this *Job) forSortedJobQueues(f func(jobQueue string)) {
	jobQueues := this.zkzone.ChildrenWithData(zk.PubsubJobQueues)
	sortedName := make([]string, 0, len(jobQueues))
	for name := range jobQueues {
		sortedName = append(sortedName, name)
	}
	sort.Strings(sortedName)

	for _, name := range sortedName {
		f(name)
	}
}

func (this *Job) printJobQueueAndActors() {
	lines := make([]string, 0)
	header := "JobQueue|Cluster|Ctime|Mtime"
	lines = append(lines, header)
	jobQueues := this.zkzone.ChildrenWithData(zk.PubsubJobQueues)
	sortedName := make([]string, 0, len(jobQueues))
	for name := range jobQueues {
		sortedName = append(sortedName, name)
	}
	sort.Strings(sortedName)

	for _, name := range sortedName {
		zdata := jobQueues[name]
		lines = append(lines, fmt.Sprintf("%s|%s|%s|%s", name,
			string(zdata.Data()), zdata.Ctime(), zdata.Mtime()))
	}
	if len(lines) > 1 {
		this.Ui.Output(columnize.SimpleFormat(lines))
	}

	lines = lines[:0]
	header = "Actor|Ctime|Mtime"
	lines = append(lines, header)
	actors := this.zkzone.ChildrenWithData(zk.PubsubActors)
	sortedName = sortedName[:0]
	for name := range actors {
		sortedName = append(sortedName, name)
	}
	sort.Strings(sortedName)

	for _, name := range sortedName {
		zdata := actors[name]
		lines = append(lines, fmt.Sprintf("%s|%s|%s", name,
			zdata.Ctime(), zdata.Mtime()))
	}
	if len(lines) > 1 {
		this.Ui.Output("")
		this.Ui.Output(columnize.SimpleFormat(lines))
	}

	lines = lines[:0]
	header = "JobQueue|Actor|Ctime|Mtime"
	lines = append(lines, header)
	jobQueueOwners := this.zkzone.ChildrenWithData(zk.PubsubJobQueueOwners)
	sortedName = sortedName[:0]
	for name := range jobQueueOwners {
		sortedName = append(sortedName, name)
	}
	sort.Strings(sortedName)

	for _, jobQueue := range sortedName {
		zdata := jobQueueOwners[jobQueue]
		lines = append(lines, fmt.Sprintf("%s|%s|%s|%s", jobQueue, string(zdata.Data()),
			zdata.Ctime(), zdata.Mtime()))
	}
	if len(lines) > 1 {
		this.Ui.Output("")
		this.Ui.Output(columnize.SimpleFormat(lines))
	}
}

func (this *Job) connectMysqlCluster(appid string) int {
	b, err := this.zkzone.KatewayJobClusterConfig()
	if err != nil {
		panic(err)
	}
	var mcc = &config.ConfigMysql{}
	if err = mcc.From(b); err != nil {
		panic(err)
	}

	this.mc = mysql.New(mcc)
	return jm.App_id(appid)
}

func (*Job) Synopsis() string {
	return "Display job/actor related znodes for PubSub system."
}

func (this *Job) Help() string {
	help := fmt.Sprintf(`
Usage: %s job [options]

    %s

Options:

    -z zone

    -c cluster

    -app <app id>
      List app's real-time and archive job table contents.

    -d <due time in seconds>
      List jobs due from now within how many seconds.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
