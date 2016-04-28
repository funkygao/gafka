package zk

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestParseStatResult(t *testing.T) {
	s := `
Zookeeper version: 3.4.6-1569965, built on 02/20/2014 09:09 GMT
Clients:
 /127.0.0.1:54276[1](queued=0,recved=0,sent=0)
 /127.0.0.1:53766[1](queued=0,recved=0,sent=0)
 /127.0.0.1:55298[1](queued=0,recved=0,sent=0)
 /0:0:0:0:0:0:0:1%0:53888[0](queued=0,recved=1,sent=0)

Latency min/avg/max: 0/0/4196
Received: 3369429
Sent: 3368840
Connections: 4
Outstanding: 0
Zxid: 0x153b
Mode: standalone
Node count: 48
	`
	stat := ParseStatResult(s)
	assert.Equal(t, "3.4.6-1569965", stat.Version)
	assert.Equal(t, "0/0/4196", stat.Latency)
	assert.Equal(t, "3369429", stat.Received)
	assert.Equal(t, "3368840", stat.Sent)
	assert.Equal(t, "4", stat.Connections)
	assert.Equal(t, "S", stat.Mode)
	assert.Equal(t, "48", stat.Znodes)
}
