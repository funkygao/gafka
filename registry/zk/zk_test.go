package zk

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
)

func init() {
	ctx.LoadFromHome()
}

func TestZkPath(t *testing.T) {
	zkzone := zk.NewZkZone(zk.DefaultConfig(ctx.DefaultZone(), ctx.ZoneZkAddrs(ctx.DefaultZone())))
	defer zkzone.Close()
	zk := New(zkzone, "1", nil)
	assert.Equal(t, "/_kateway/ids/local", Root("local"))
	assert.Equal(t, "/_kateway/ids/local/1", zk.mypath())
}
