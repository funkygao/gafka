package command

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	mandb "github.com/funkygao/gafka/cmd/kateway/manager/mysql"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/go-ozzo/ozzo-dbx"
	"github.com/olekukonko/tablewriter"
)

type TopicInfo struct {
	TopicId        string `db:"TopicId"`
	AppId          string `db:"AppId"`
	TopicName      string `db:"TopicName"`
	TopicIntro     string `db:"TopicIntro"`
	KafkaTopicName string `db:"KafkaTopicName"`
}

type Verify struct {
	Ui  cli.Ui
	Cmd string

	zone       string
	zkzone     *zk.ZkZone
	cluster    string
	zkclusters map[string]*zk.ZkCluster // cluster:zkcluster
	confirmed  bool
	mode       string

	db *dbx.DB

	topics []TopicInfo

	problemeticTopics map[string]struct{}
	kafkaTopics       map[string]string        // topic:cluster
	kfkClients        map[string]sarama.Client // cluster:client
	psubClient        map[string]sarama.Client // cluster:client
}

func (this *Verify) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("verify", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "bigtopic", "")
	cmdFlags.BoolVar(&this.confirmed, "go", false, "")
	cmdFlags.StringVar(&this.mode, "mode", "p", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z").
		invalid(args) {
		return 2
	}

	cf := mandb.DefaultConfig(this.zone)
	manager.Default = mandb.New(cf)

	ensureZoneValid(this.zone)
	this.zkzone = zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	this.kafkaTopics = make(map[string]string)
	this.problemeticTopics = make(map[string]struct{})
	this.kfkClients = make(map[string]sarama.Client)
	this.psubClient = make(map[string]sarama.Client)
	this.zkclusters = make(map[string]*zk.ZkCluster)
	this.zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		this.zkclusters[zkcluster.Name()] = zkcluster

		kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
		swallow(err)

		this.kfkClients[zkcluster.Name()] = kfk
		if this.cluster == zkcluster.Name() {
			this.psubClient[zkcluster.Name()] = kfk
		}

		topics, err := kfk.Topics()
		swallow(err)

		for _, t := range topics {
			if _, present := this.kafkaTopics[t]; present {
				this.problemeticTopics[t] = struct{}{}
			}

			this.kafkaTopics[t] = zkcluster.Name()
		}
	})

	for topic, _ := range this.problemeticTopics {
		this.Ui.Warn(fmt.Sprintf("dup clusters found for topic: %s", topic))
	}

	mysqlDsns := map[string]string{
		"prod": "user_pubsub:p0nI7mEL6OLW@tcp(m3342.wdds.mysqldb.com:3342)/pubsub?charset=utf8&timeout=10s",
		"sit":  "pubsub:pubsub@tcp(10.209.44.12:10043)/pubsub?charset=utf8&timeout=10s",
		"test": "pubsub:pubsub@tcp(10.209.44.14:10044)/pubsub?charset=utf8&timeout=10s",
	}

	this.loadFromManager(mysqlDsns[this.zone])
	switch this.mode {
	case "p":
		this.verifyPub()

	case "s":
		this.verifySub()

	case "t":
		this.showTable()
	}

	return
}

func (this *Verify) showTable() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"AppId", "PubSub Topic", "Desc", "Kafka Topic", "?"})

	for _, t := range this.topics {
		kafkaTopic := t.KafkaTopicName
		if kafkaTopic == "" {
			// try find its counterpart in raw kafka
			var q *dbx.Query
			if _, present := this.kafkaTopics[t.TopicName]; present {
				kafkaTopic = "[" + t.TopicName + "]"

				q = this.db.NewQuery(fmt.Sprintf("UPDATE topics SET KafkaTopicName='%s' WHERE TopicId=%s",
					t.TopicName, t.TopicId))
				this.Ui.Output(q.SQL())
			}

			if q != nil && this.confirmed {
				_, err := q.Execute()
				swallow(err)
			}
		}

		problem := "N"
		if _, present := this.problemeticTopics[kafkaTopic]; present {
			problem = color.Yellow("Y")
		}

		table.Append([]string{t.AppId, t.TopicName, t.TopicIntro, kafkaTopic, problem})
	}

	table.Render()
}

func (this *Verify) verifyPub() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Kafka", "Stock", "PubSub", "Stock", "Diff", "?"})
	for _, t := range this.topics {
		if t.KafkaTopicName == "" {
			continue
		}
		kafkaCluster := this.kafkaTopics[t.KafkaTopicName]
		if kafkaCluster == "" {
			this.Ui.Warn(fmt.Sprintf("invalid kafka topic: %s", t.KafkaTopicName))
			continue
		}

		psubTopic := manager.Default.KafkaTopic(t.AppId, t.TopicName, "v1")
		offsets := this.pubOffsetDiff(t.KafkaTopicName, kafkaCluster,
			psubTopic, this.cluster)
		var diff string
		if offsets[0] == 0 && offsets[1] != 0 {
			diff = color.Yellow("%d", offsets[1]-offsets[0])
		} else if math.Abs(float64(offsets[0]-offsets[1])) < 20 {
			diff = color.Green("%d", offsets[1]-offsets[0])
		} else {
			diff = color.Red("%d", offsets[1]-offsets[0])
		}

		problem := "N"
		if _, present := this.problemeticTopics[t.KafkaTopicName]; present {
			problem = color.Yellow("Y")
		}

		table.Append([]string{
			t.KafkaTopicName, fmt.Sprintf("%d", offsets[0]),
			t.TopicName, fmt.Sprintf("%d", offsets[1]), diff, problem})
	}

	table.Render()
}

func (this *Verify) pubOffsetDiff(kafkaTopic, kafkaCluster, psubTopic, psubCluster string) []int64 {
	kfk := this.kfkClients[kafkaCluster]
	psub := this.psubClient[psubCluster]

	kp, err := kfk.Partitions(kafkaTopic)
	swallow(err)
	kN := int64(0)
	for _, p := range kp {
		hi, err := kfk.GetOffset(kafkaTopic, p, sarama.OffsetNewest)
		swallow(err)

		lo, err := kfk.GetOffset(kafkaTopic, p, sarama.OffsetOldest)
		swallow(err)

		kN += (hi - lo)
	}

	psp, err := psub.Partitions(psubTopic)
	if err != nil {
		this.Ui.Error(fmt.Sprintf("psub: %s", psubTopic))
		return []int64{0, 0}
	}
	pN := int64(0)
	for _, p := range psp {
		hi, err := psub.GetOffset(psubTopic, p, sarama.OffsetNewest)
		swallow(err)

		lo, err := psub.GetOffset(psubTopic, p, sarama.OffsetOldest)
		swallow(err)

		pN += (hi - lo)
	}

	return []int64{kN, pN}
}

func (this *Verify) verifySub() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Topic", "Kafka Consumer Group", "PubSub Consumer Group"})
	for _, t := range this.topics {
		if t.KafkaTopicName == "" {
			continue
		}
		kafkaCluster := this.kafkaTopics[t.KafkaTopicName]
		if kafkaCluster == "" {
			this.Ui.Warn(fmt.Sprintf("invalid kafka topic: %s", t.KafkaTopicName))
			continue
		}

		psubTopic := manager.Default.KafkaTopic(t.AppId, t.TopicName, "v1")
		psubGroups := this.fetchConsumerGroups(psubTopic, this.cluster)
		kfkGroups := this.fetchConsumerGroups(t.KafkaTopicName, kafkaCluster)
		pN, kN := len(psubGroups), len(kfkGroups)
		max := int(math.Max(float64(pN), float64(kN)))
		for i := 0; i < max; i++ {
			var kg, pg string
			if i >= pN {
				pg = ""
			} else {
				pg = psubGroups[i]
			}

			if i >= kN {
				kg = ""
			} else {
				kg = kfkGroups[i]
			}

			table.Append([]string{t.KafkaTopicName, kg, pg})
		}

	}
	table.Render()
}

func (this *Verify) fetchConsumerGroups(topic, cluster string) []string {
	consumerGroups := this.zkclusters[cluster].ConsumerGroups()
	sortedGroups := make([]string, 0, len(consumerGroups))
	for group, _ := range consumerGroups {
		sortedGroups = append(sortedGroups, group)
	}

	r := make([]string, 0, len(sortedGroups))
	sort.Strings(sortedGroups)
	includedGroups := make(map[string]struct{})
	for _, group := range sortedGroups {
		consumers := consumerGroups[group]
		if len(consumers) == 0 {
			// show only online consumers
			continue
		}

		for _, c := range consumers {
			if c.Topics()[0] == topic {
				if _, present := includedGroups[group]; !present {
					r = append(r, group)
					includedGroups[group] = struct{}{}
				}
			}
		}
	}

	return r
}

func (this *Verify) loadFromManager(dsn string) {
	db, err := dbx.Open("mysql", dsn)
	swallow(err)

	this.db = db

	// TODO fetch from topics_version
	q := db.NewQuery("SELECT TopicId, AppId, TopicName, TopicIntro, KafkaTopicName FROM topics WHERE Status = 1 ORDER BY AppId, CategoryId, TopicName")
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

    -mode <p|s|t>   

    -go
      Confirmed to update KafkaTopicName in table topics.


`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
