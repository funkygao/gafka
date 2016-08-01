package mysql

type applicationRecord struct {
	AppId, Cluster, AppSecret string
}

type appTopicRecord struct {
	AppId, TopicName, Status string
}

type appSubscribeRecord struct {
	AppId, TopicName string
}

type appConsumerGroupRecord struct {
	AppId, GroupName string
}

type shadowQueueRecord struct {
	HisAppId, TopicName, Ver string
	MyAppid, Group           string
}

type deadPartitionRecord struct {
	KafkaTopic  string
	PartitionId int32
}

type topicSchemaRecord struct {
	AppId, TopicName, Ver string
	Schema                string
}
