package zkmeta

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
)

func init() {
	ctx.LoadFromHome()
}

func TestAll(t *testing.T) {
	zkzone := zk.NewZkZone(zk.DefaultConfig(ctx.DefaultZone(), ctx.ZoneZkAddrs(ctx.DefaultZone())))
	defer zkzone.Close()
	z := New(DefaultConfig(), zkzone)
	z.Start()

	assert.Equal(t, "/kafka_pubsub", z.ZkChroot("me"))
	assert.Equal(t, []string{"localhost:2181"}, z.ZkAddrs())

	t.Logf("%+v", z.BrokerList("me"))
	t.Logf("%+v", z.TopicPartitions("me", "app1.foobar.v1"))
	t.Logf("%+v", z.ZkCluster("me"))
	t.Logf("%+v", z.OnlineConsumersCount("me", "app1.foobar.v1", "group"))
}
