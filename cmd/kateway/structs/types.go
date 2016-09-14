package structs

import (
	"fmt"
)

type ClusterTopic struct {
	Cluster, Topic string
}

func (ct ClusterTopic) String() string {
	return ct.Cluster + "/" + ct.Topic
}

type AppTopic struct {
	AppID, Topic string
}

func (at AppTopic) String() string {
	return at.AppID + "/" + at.Topic
}

type AppTopicVer struct {
	AppID, Topic, Ver string
}

func (atv AppTopicVer) String() string {
	return atv.AppID + "/" + atv.Topic + "/" + atv.Ver
}

type TopicPartition struct {
	Topic       string
	PartitionID int32
}

func (tp TopicPartition) Str1ing() string {
	return fmt.Sprintf("%s/%d", tp.Topic, tp.PartitionID)
}

type AppGroup struct {
	AppID, Group string
}

func (ag *AppGroup) String() string {
	return ag.AppID + "/" + ag.Group
}
