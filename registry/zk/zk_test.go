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
	zk := New(zkzone)
	id := "1"
	assert.Equal(t, "/_kateway/ids/local/1", zk.mypath(id))

	data := []byte("foo, bar")
	err := zk.Register(id, data)
	assert.Equal(t, nil, err)
	defer zk.Deregister(id, data)

	ok, err := zk.Registered(id)
	assert.Equal(t, true, ok)
	assert.Equal(t, nil, err)
}
