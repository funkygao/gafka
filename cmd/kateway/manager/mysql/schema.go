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
	AppId, TopicName, Ver string
	PartitionId           int32
}
