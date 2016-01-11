package zk

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/ctx"
)

func init() {
	ctx.LoadFromHome()
}

func TestZkPath(t *testing.T) {
	zk := New("local", "1", nil)
	assert.Equal(t, "/_kateway/ids/local", Root("local"))
	assert.Equal(t, "/_kateway/ids/local/1", zk.mypath())
}
