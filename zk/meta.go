package zk

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/funkygao/golib/gofmt"
)

type zkTimestamp int64

func (this zkTimestamp) Time() time.Time {
	return time.Unix(int64(this)/int64(1000), 0)
}

type zkData struct {
	data  []byte
	mtime zkTimestamp
}

type ConsumerMeta struct {
	Group          string
	Online         bool
	Topic          string
	PartitionId    string
	Mtime          zkTimestamp
	ConsumerOffset int64
	ProducerOffset int64
	Lag            int64
	ConsumerZnode  *ConsumerZnode
}

type ControllerMeta struct {
	Broker *BrokerZnode
	Mtime  zkTimestamp
	Epoch  string
}

func (c *ControllerMeta) String() string {
	return fmt.Sprintf("%s epoch:%s/%s %s", c.Broker.Id, c.Epoch,
		gofmt.PrettySince(c.Mtime.Time()), c.Broker.String())
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

func (c *ConsumerZnode) from(zkData []byte) {
	if err := json.Unmarshal(zkData, c); err != nil {
		panic(err)
	}
}

func (c *ConsumerZnode) Host() string {
	// consumerId: $group_$hostname-$timestamp-$uuid
	dashN := 0
	var lo, hi int
	for hi = len(c.Id) - 1; hi >= 0; hi-- {
		if c.Id[hi] == '-' {
			dashN++
			if dashN == 2 {
				break
			}
		}
	}

	for lo = hi; c.Id[lo] != '_'; lo-- {
	}

	return c.Id[lo+1 : hi]
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

func (b BrokerZnode) String() string {
	return fmt.Sprintf("%s:%d ver:%d uptime:%s",
		b.Host, b.Port,
		b.Version,
		gofmt.PrettySince(b.Uptime()))
}

func (b *BrokerZnode) Uptime() time.Time {
	return TimestampToTime(b.Timestamp)
}

func (b *BrokerZnode) from(zkData []byte) {
	if err := json.Unmarshal(zkData, b); err != nil {
		panic(err)
	}
}

func (b *BrokerZnode) Addr() string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}
