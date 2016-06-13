package zk

import (
	"encoding/json"
	"fmt"
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
	Timestamp    string         `json:"timestamp"`
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

func (c *ConsumerZnode) Topics() []string {
	r := make([]string, 0, len(c.Subscription))
	for topic, _ := range c.Subscription {
		r = append(r, topic)
	}
	return r
}

func (c *ConsumerZnode) Uptime() time.Time {
	return TimestampToTime(c.Timestamp)
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
	return fmt.Sprintf("%s ver:%d uptime:%s",
		b.NamedAddr(),
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

func (this *BrokerZnode) NamedAddr() string {
	dns, present := ctx.ReverseDnsLookup(this.Host, this.Port)
	if !present {
		return this.Addr()
	}

	return fmt.Sprintf("%s:%d", dns, this.Port)
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
