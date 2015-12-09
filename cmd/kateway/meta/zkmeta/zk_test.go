package zkmeta

import (
	"testing"
	"time"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/ctx"
)

func init() {
	ctx.LoadConfig("/etc/kateway.cf")
}

func TestAll(t *testing.T) {
	z := New("local", time.Second*10)
	z.Start()

	assert.Equal(t, "/kafka_pubsub", z.ZkChroot("me"))
	assert.Equal(t, []string{"localhost:2181"}, z.ZkAddrs())

	t.Logf("%+v", z.BrokerList("me"))
	t.Logf("%+v", z.Partitions("me", "app1.foobar.v1"))
	t.Logf("%+v", z.ZkCluster("me"))
	t.Logf("%+v", z.OnlineConsumersCount("me", "app1.foobar.v1", "group"))
	t.Logf("%+v", z.Clusters())
}
