package zk

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestDbusPathRelated(t *testing.T) {
	cluster := "foo"
	assert.Equal(t, "/dbus/foo/cluster", DbusClusterRoot(cluster))
	assert.Equal(t, "/dbus/foo/checkpoint", DbusCheckpointRoot(cluster))
	assert.Equal(t, "/dbus/foo/conf", DbusConfig(cluster))
	assert.Equal(t, "/dbus/foo/conf.d", DbusConfigDir(cluster))
}
