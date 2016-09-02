package disk

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestClusterTopicDir(t *testing.T) {
	ct := clusterTopic{"cluster", "topic"}
	assert.Equal(t, "/var/cluster", ct.ClusterDir("/var"))
	assert.Equal(t, "/cluster/topic", ct.TopicDir("/"))
}
