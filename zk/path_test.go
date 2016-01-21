package zk

import (
	"testing"

	"github.com/funkygao/assert"
)

func createClusterForTest() *ZkCluster {
	return &ZkCluster{
		name: "cluster-test",
		path: "/test",
	}
}

func TestZkClusterPath(t *testing.T) {
	c := createClusterForTest()
	assert.Equal(t, "/test/brokers/ids", c.brokerIdsRoot())
	assert.Equal(t, "/test/controller", c.controllerPath())
	assert.Equal(t, "/test/controller_epoch", c.controllerEpochPath())
	assert.Equal(t, "/test/brokers/topics/t1/partitions",
		c.partitionsPath("t1"))
	assert.Equal(t, "/test/brokers/topics/t1/partitions/2/state",
		c.partitionStatePath("t1", 2))
	assert.Equal(t, "/test/brokers/topics", c.topicsRoot())
	assert.Equal(t, "/test/brokers/ids", c.brokerIdsRoot())
	assert.Equal(t, "/test/brokers/ids/2", c.brokerPath(2))
	assert.Equal(t, "/test/consumers/console-group",
		c.ConsumerGroupRoot("console-group"))
	assert.Equal(t, "/test/consumers", c.consumerGroupsRoot())
	assert.Equal(t, "/test/consumers/console-group/ids",
		c.consumerGroupIdsPath("console-group"))
	assert.Equal(t, "/test/consumers/console-group/offsets",
		c.ConsumerGroupOffsetPath("console-group"))
	assert.Equal(t, "/test/consumers/console-group/offsets/t1",
		c.consumerGroupOffsetOfTopicPath("console-group", "t1"))
	assert.Equal(t, "/test/consumers/console-group/owners/t1",
		c.consumerGroupOwnerOfTopicPath("console-group", "t1"))

}

func TestClusterPath(t *testing.T) {
	assert.Equal(t, "/_kafka_clusters/test-cluster", ClusterPath("test-cluster"))
}
