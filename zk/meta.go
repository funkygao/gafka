package zk

import (
	"encoding/json"
	"fmt"
	"time"
)

type zkTimestamp int64

func (this zkTimestamp) Time() time.Time {
	return time.Unix(int64(this)/int64(1000), 0)
}

type zkData struct {
	data      []byte
	timestamp zkTimestamp
}

type Consumer struct {
	Group          string
	Online         bool
	Topic          string
	PartitionId    string
	Timestamp      zkTimestamp
	ConsumerOffset int64
	ProducerOffset int64
	Lag            int64
}

type Controller struct {
	Broker *BrokerZnode
	Epoch  string
}

// FIXME should not be consider padding
func (c *Controller) String() string {
	return fmt.Sprintf("%s epoch:%s %s", c.Broker.Id, c.Epoch, c.Broker.String())
}

type ConsumerZnode struct {
	Version      int            `json:"version"`
	Subscription map[string]int `json:"subscription"` // topic:count
	Pattern      string         `json:"pattern"`
	Timestamp    string         `json:"timestamp"`
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
		time.Since(TimestampToTime(b.Timestamp)))
}

func (b *BrokerZnode) from(zkData []byte) {
	if err := json.Unmarshal(zkData, b); err != nil {
		panic(err)
	}
}

func (b *BrokerZnode) Addr() string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}
