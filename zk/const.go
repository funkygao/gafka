package zk

const (
	clusterRoot     = "/_kafka_clusters"
	zkPathSeperator = "/"

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
