package zk

import (
	"fmt"
)

const (
	clusterRoot     = "/_kafka_clusters"
	clusterInfoRoot = "/_kafa_clusters_info"

	katewayMysqlPath = "/kateway/mysql"
	KatewayIdsRoot   = "/_kateway/ids"

	ConsumersPath           = "/consumers"
	BrokerIdsPath           = "/brokers/ids"
	BrokerTopicsPath        = "/brokers/topics"
	ControllerPath          = "/controller"
	ControllerEpochPath     = "/controller_epoch"
	BrokerSequenceIdPath    = "/brokers/seqid"
	EntityConfigChangesPath = "/config/changes"
	TopicConfigPath         = "/config/topics"
	EntityConfigPath        = "/config"
	DeleteTopicsPath        = "/admin/delete_topics"
)

func clusterPath(cluster string) string {
	return fmt.Sprintf("%s/%s", clusterRoot, cluster)
}

func (this *ZkCluster) controllerPath() string {
	return this.path + ControllerPath
}

func (this *ZkCluster) cluserInfoPath() string {
	return fmt.Sprintf("%s/%s", clusterInfoRoot, this.name)
}

func (this *ZkCluster) controllerEpochPath() string {
	return this.path + ControllerEpochPath
}

func (this *ZkCluster) partitionsPath(topic string) string {
	return fmt.Sprintf("%s%s/%s/partitions", this.path, BrokerTopicsPath, topic)
}

func (this *ZkCluster) partitionStatePath(topic string, partitionId int32) string {
	return fmt.Sprintf("%s/%d/state", this.partitionsPath(topic), partitionId)
}

func (this *ZkCluster) topicsRoot() string {
	return this.path + BrokerTopicsPath
}

func (this *ZkCluster) brokerIdsRoot() string {
	return this.path + BrokerIdsPath
}

func (this *ZkCluster) brokerPath(id int) string {
	return fmt.Sprintf("%s/%d", this.brokerIdsRoot(), id)
}

func (this *ZkCluster) consumerGroupsRoot() string {
	return this.path + ConsumersPath
}

func (this *ZkCluster) ConsumerGroupRoot(group string) string {
	return this.path + ConsumersPath + "/" + group
}

func (this *ZkCluster) consumerGroupIdsPath(group string) string {
	return this.ConsumerGroupRoot(group) + "/ids"
}

func (this *ZkCluster) ConsumerGroupOffsetPath(group string) string {
	return this.ConsumerGroupRoot(group) + "/offsets"
}

func (this *ZkCluster) consumerGroupOffsetOfTopicPath(group, topic string) string {
	return this.ConsumerGroupOffsetPath(group) + "/" + topic
}

func (this *ZkCluster) consumerGroupOwnerOfTopicPath(group, topic string) string {
	return this.ConsumerGroupRoot(group) + "/owners/" + topic
}
