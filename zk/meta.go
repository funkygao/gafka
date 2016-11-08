package zk

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/gofmt"
)

type ZkTimestamp int64

func (this ZkTimestamp) Time() time.Time {
	return time.Unix(int64(this)/int64(1000), 0)
}

type zkData struct {
	data  []byte
	mtime ZkTimestamp
	ctime ZkTimestamp
}

func (this *zkData) Data() []byte {
	return this.data
}

func (this *zkData) Mtime() time.Time {
	return this.mtime.Time()
}

func (this *zkData) Ctime() time.Time {
	return this.ctime.Time()
}

type ConsumerMeta struct {
	Group          string
	Online         bool
	Topic          string
	PartitionId    string
	Mtime          ZkTimestamp
	ConsumerOffset int64
	OldestOffset   int64
	ProducerOffset int64 // newest offset
	Lag            int64
	ConsumerZnode  *ConsumerZnode
}

type WebhookMeta struct {
	Cluster   string   `json:"cluster"`
	Endpoints []string `json:"endpoints"`
}

func (this *WebhookMeta) From(b []byte) error {
	return json.Unmarshal(b, this)
}

func (this *WebhookMeta) Bytes() []byte {
	b, _ := json.Marshal(this)
	return b
}

type ControllerMeta struct {
	Broker *BrokerZnode
	Mtime  ZkTimestamp
	Epoch  string
}

func (c *ControllerMeta) String() string {
	return fmt.Sprintf("%s epoch:%s/%s %s", c.Broker.Id, c.Epoch,
		gofmt.PrettySince(c.Mtime.Time()), c.Broker.String())
}

type TopicZnode struct {
	Name       string           `json:-`
	Version    int              `json:"version"`
	Partitions map[string][]int `json:"partitions"` // {partitionId: replicas}
}

type ConsumerZnode struct {
	Id           string         `json:-`
	Version      int            `json:"version"`
	Subscription map[string]int `json:"subscription"` // topic:count
	Pattern      string         `json:"pattern"`
	Timestamp    interface{}    `json:"timestamp"`
}

func newConsumerZnode(id string) *ConsumerZnode {
	return &ConsumerZnode{Id: id}
}

func (c *ConsumerZnode) from(zkData []byte) error {
	return json.Unmarshal(zkData, c)
}

func (c *ConsumerZnode) Host() string {
	return hostOfConsumer(c.Id)
}

func (c *ConsumerZnode) ClientRealIP() (ip string) {
	ip = c.Host()
	if strings.Contains(ip, "@") {
		// Sub by kateway
		// e,g XXCS-200040100@10.10.1.2
		p := strings.SplitN(ip, "@", 2)
		if len(p) != 2 {
			return
		}

		return p[1]
	}

	return
}

func (c *ConsumerZnode) Topics() []string {
	r := make([]string, 0, len(c.Subscription))
	for topic, _ := range c.Subscription {
		r = append(r, topic)
	}
	return r
}

func (c *ConsumerZnode) Uptime() time.Time {
	var ts string
	switch t := c.Timestamp.(type) {
	case int:
		ts = strconv.Itoa(t)
	case float32:
		ts = strconv.Itoa(int(t))
	case float64:
		ts = strconv.Itoa(int(t))
	case string:
		ts = t
	}
	return TimestampToTime(ts)
}

func (c *ConsumerZnode) String() string {
	return fmt.Sprintf("host:%s, sub:%+v, uptime:%s",
		c.Host(), c.Subscription, gofmt.PrettySince(c.Uptime()))
}

type BrokerZnode struct {
	Id        string   `json:-`
	JmxPort   int      `json:"jmx_port"`
	Timestamp string   `json:"timestamp"`
	Endpoints []string `json:"endpoints"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Version   int      `json:"version"`
}

func newBrokerZnode(id string) *BrokerZnode {
	return &BrokerZnode{Id: id}
}

func (b BrokerZnode) NamedString() string {
	addr, _ := b.NamedAddr()
	return fmt.Sprintf("%s ver:%d uptime:%s",
		addr,
		b.Version,
		gofmt.PrettySince(b.Uptime()))
}

func (b BrokerZnode) String() string {
	return fmt.Sprintf("%s ver:%d uptime:%s",
		b.Addr(),
		b.Version,
		gofmt.PrettySince(b.Uptime()))
}

func (b *BrokerZnode) Uptime() time.Time {
	return TimestampToTime(b.Timestamp)
}

func (b *BrokerZnode) from(zkData []byte) error {
	return json.Unmarshal(zkData, b)
}

func (b *BrokerZnode) Addr() string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

func (this *BrokerZnode) NamedAddr() (string, bool) {
	dns, present := ctx.ReverseDnsLookup(this.Host, this.Port)
	if !present {
		return this.Addr(), false
	}

	return fmt.Sprintf("%s:%d", dns, this.Port), true
}

type BrokerInfo struct {
	Id   int    `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

func (this *BrokerInfo) Addr() string {
	return fmt.Sprintf("%s:%d", this.Host, this.Port)
}

func (this *BrokerInfo) NamedAddr() string {
	dns, present := ctx.ReverseDnsLookup(this.Host, this.Port)
	if !present {
		return this.Addr()
	}

	return fmt.Sprintf("%s:%d", dns, this.Port)
}

type PartitionOffset struct {
	Cluster             string
	Topic               string
	Partition           int32
	Offset              int64
	Timestamp           int64
	Group               string
	TopicPartitionCount int
}

type TopicConfigMeta struct {
	Config string
	Ctime  time.Time
	Mtime  time.Time
}

type KatewayMeta struct {
	Id        string `json:"id"`
	Zone      string `json:"zone"`
	Ver       string `json:"ver"`
	Build     string `json:"build"`
	BuiltAt   string `json:"builtat"`
	Arch      string `json:"arch"`
	Host      string `json:"host"`
	Ip        string `json:"ip"`
	Cpu       string `json:"cpu"`
	PubAddr   string `json:"pub"`
	SPubAddr  string `json:"spub"`
	SubAddr   string `json:"sub"`
	SSubAddr  string `json:"ssub"`
	ManAddr   string `json:"man"`
	SManAddr  string `json:"sman"`
	DebugAddr string `json:"debug"`

	Ctime time.Time `json:"-"`
}

type KguardMeta struct {
	Host       string
	Candidates int
	Ctime      time.Time
}
