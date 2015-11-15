package zk

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestBrokerFrom(t *testing.T) {
	var b Broker
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
	tm := zkTimestamp(1447157138058)
	t.Logf("%v", tm.Time())
	assert.Equal(t, 2015, tm.Time().Year())
	assert.Equal(t, 10, tm.Time().Day())
}
