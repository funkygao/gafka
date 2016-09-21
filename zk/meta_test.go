package zk

import (
	"testing"

	"github.com/funkygao/assert"
	log "github.com/funkygao/log4go"
)

func init() {
	log.Disable()
}

func TestBrokerZnodeFrom(t *testing.T) {
	var b BrokerZnode
	b.Id = "5"
	b.from([]byte(`{"jmx_port":-1,"timestamp":"1447157138058","host":"192.168.3.5","version":1,"port":9092}`))
	assert.Equal(t, 9092, b.Port)
	assert.Equal(t, "192.168.3.5", b.Host)
	assert.Equal(t, 1, b.Version)
	assert.Equal(t, -1, b.JmxPort)
	assert.Equal(t, "1447157138058", b.Timestamp)
	assert.Equal(t, "5", b.Id)
}

func TestZkTimestamp(t *testing.T) {
	tm := ZkTimestamp(1447157138058)
	t.Logf("%v", tm.Time())
	assert.Equal(t, 2015, tm.Time().Year())
	assert.Equal(t, 10, tm.Time().Day())
}

func TestConsumerZnode(t *testing.T) {
	c := newConsumerZnode("cloudparkingGroup_orderMsg_BJS0-D134-018-1447657979158-fa9d1dc8")
	assert.Equal(t, "BJS0-D134-018", c.Host())

	c = newConsumerZnode("DC-243001184@192.168.10.134:33f3a781-1dd5-488d-84fa-f8d3febce170")
	assert.Equal(t, "192.168.10.134", c.ClientRealIP())
}

func TestConsumerZnodeFrom(t *testing.T) {
	c := newConsumerZnode("consumerId")
	err := c.from([]byte(`{"version":1,"subscription":{"wifi_detail_log": 1, "wifi_store_inout_detail_log": 1, "wifi_portal_log": 1},"pattern":"white_list","timestamp":"1473402575029"}`))
	assert.Equal(t, nil, err)
}

func TestWebhook(t *testing.T) {
	var hook WebhookMeta
	hook.Cluster = "trade"
	hook.Endpoints = []string{"http://localhost:9876"}
	t.Logf("%s", string(hook.Bytes()))
}
