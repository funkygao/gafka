package structs

type ClusterTopic struct {
	Cluster, Topic string
}

type AppTopic struct {
	AppID, Topic string
}

type AppTopicVer struct {
	AppID, Topic, Ver string
}

type TopicPartition struct {
	Topic       string
	PartitionID int32
}

type AppGroup struct {
	AppID, Group string
}
