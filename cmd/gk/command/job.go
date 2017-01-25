package command

import (
	"flag"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/funkygao/columnize"
	"github.com/funkygao/fae/config"
	"github.com/funkygao/fae/servant/mysql"
	"github.com/funkygao/gafka/cmd/kateway/job"
	jm "github.com/funkygao/gafka/cmd/kateway/job/mysql"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/manager/dummy"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/gorequest"
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
		zone    string
		appid   string
		initJob string
	)
	cmdFlags := flag.NewFlagSet("job", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&appid, "app", "", "")
	cmdFlags.IntVar(&this.due, "d", 0, "")
	cmdFlags.StringVar(&initJob, "init", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.zkzone = zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	if initJob != "" {
		this.initializeJob(initJob)
		return
	}

	if appid != "" {
		this.displayAppJobs(appid)
		return
	}

	this.printResourcesAndActors()

	return
}

func (this *Job) initializeJob(name string) {
	tuples := strings.Split(name, ".")
	if len(tuples) != 3 {
		panic("invalid name")
	}

	kws, err := this.zkzone.KatewayInfos()
	swallow(err)
	if len(kws) < 1 {
		panic("no live kateway instance")
	}

	uri := kws[0].ManAddr
	if !strings.HasPrefix(uri, "http:") {
		uri = fmt.Sprintf("http://%s", uri)
	}
	uri = fmt.Sprintf("%s/v1/jobs/%s/%s/%s", uri, tuples[0], tuples[1], tuples[2])
	this.Ui.Info(uri)

	req := gorequest.New()
	zone := ctx.Zone(this.zkzone.Name())
	resp, _, errs := req.Post(uri).Set("Appid", zone.AdminUser).Set("Pubkey", zone.AdminPass).End()
	if resp.StatusCode != http.StatusCreated {
		this.Ui.Error(resp.Status)
	} else if len(errs) > 0 {
		for _, err = range errs {
			this.Ui.Error(err.Error())
		}
	} else {
		this.Ui.Info(fmt.Sprintf("%s initialized", name))
	}

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

		sqlRealTime := fmt.Sprintf("SELECT job_id,payload,due_time FROM %s ORDER BY due_time DESC", table)
		if this.due > 0 {
			sqlRealTime = fmt.Sprintf("SELECT job_id,payload,due_time FROM %s WHERE due_time<=%d ORDER BY due_time DESC",
				table, time.Now().Unix()+int64(this.due))
		}
		rows, err := this.mc.Query(jm.AppPool, table, aid, sqlRealTime)
		swallow(err)

		var item job.JobItem
		for rows.Next() {
			err = rows.Scan(&item.JobId, &item.Payload, &item.DueTime)
			swallow(err)
			lines = append(lines, fmt.Sprintf("%s|RT|%d|%d|%s", topic, item.JobId, item.DueTime, item.PayloadString(50)))
		}
		rows.Close()

		sqlArchive := fmt.Sprintf("SELECT job_id,payload,due_time FROM %s ORDER BY due_time ASC LIMIT 100", archiveTable)
		rows, err = this.mc.Query(jm.AppPool, table, aid, sqlArchive)
		swallow(err)

		for rows.Next() {
			err = rows.Scan(&item.JobId, &item.Payload, &item.DueTime)
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

func (this *Job) printResourcesAndActors() {
	lines := make([]string, 0)
	header := "Topic|Cluster|Ctime|Mtime"
	lines = append(lines, header)
	// jobs
	jobQueues := this.zkzone.ChildrenWithData(zk.PubsubJobQueues)
	sortedName := make([]string, 0, len(jobQueues))
	for name := range jobQueues {
		sortedName = append(sortedName, name)
	}
	sort.Strings(sortedName)

	manager.Default = dummy.New("")
	for _, topic := range sortedName {
		zdata := jobQueues[topic]
		if appid := manager.Default.TopicAppid(topic); appid == "" {
			lines = append(lines, fmt.Sprintf("?%s|%s|%s|%s", topic,
				string(zdata.Data()), zdata.Ctime(), zdata.Mtime()))
		} else {
			lines = append(lines, fmt.Sprintf("%s|%s|%s|%s", topic,
				string(zdata.Data()), zdata.Ctime(), zdata.Mtime()))
		}

	}
	if len(lines) > 1 {
		this.Ui.Info("Jobs")
		this.Ui.Output(columnize.SimpleFormat(lines))
	}

	// webhooks
	lines = lines[:0]
	header = "Topic|Endpoints|Ctime|Mtime"
	lines = append(lines, header)
	webhoooks := this.zkzone.ChildrenWithData(zk.PubsubWebhooks)
	sortedName = sortedName[:0]
	for name := range webhoooks {
		sortedName = append(sortedName, name)
	}
	sort.Strings(sortedName)

	for _, topic := range sortedName {
		zdata := webhoooks[topic]
		var hook zk.WebhookMeta
		hook.From(zdata.Data())
		if appid := manager.Default.TopicAppid(topic); appid == "" {
			lines = append(lines, fmt.Sprintf("?%s|%+v|%s|%s", topic, hook.Endpoints,
				zdata.Ctime(), zdata.Mtime()))
		} else {
			lines = append(lines, fmt.Sprintf("%s|%+v|%s|%s", topic, hook.Endpoints,
				zdata.Ctime(), zdata.Mtime()))
		}
	}
	if len(lines) > 1 {
		this.Ui.Info("Webhooks")
		this.Ui.Output(columnize.SimpleFormat(lines))
	}

	// actors
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
		this.Ui.Info("Actors")
		this.Ui.Output(columnize.SimpleFormat(lines))
	}

	// job <-> actor
	lines = lines[:0]
	header = "Topic|Actor|Ctime|Mtime"
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
		this.Ui.Info("Job<->Actor")
		this.Ui.Output(columnize.SimpleFormat(lines))
	}

	// webhook <-> actor
	lines = lines[:0]
	header = "Topic|Actor|Ctime|Mtime"
	lines = append(lines, header)
	webhookOwners := this.zkzone.ChildrenWithData(zk.PubsubWebhookOwners)
	sortedName = sortedName[:0]
	for name := range webhookOwners {
		sortedName = append(sortedName, name)
	}
	sort.Strings(sortedName)

	for _, topic := range sortedName {
		zdata := webhookOwners[topic]
		lines = append(lines, fmt.Sprintf("%s|%s|%s|%s", topic, string(zdata.Data()),
			zdata.Ctime(), zdata.Mtime()))
	}
	if len(lines) > 1 {
		this.Ui.Info("Webhook<->Actor")
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

    -init job
      Register a job topic.
      e,g.
        gk job -init 100.foobar.v2

    -app <app id>
      List app's real-time and archive job table contents.

    -d <due time in seconds>
      List jobs due from now within how many seconds.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
