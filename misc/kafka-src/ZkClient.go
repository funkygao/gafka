package main

type ZkClient struct {
}

func initZk() *ZkClient {
	// setup common znode paths
	for path := range []string{ConsumersPath, BrokerIdsPath, BrokerTopicsPath,
		TopicConfigChangesPath, TopicConfigPath, DeleteTopicsPath} {
		makeSurePersistentPathExists(path)
	}
	return &ZkClient{}
}

func (this *ZkClient) FetchAllTopicConfigs() {

}

func (this *ZkClient) getAllBrokersInCluster() []Broker {

}

func (this *ZkClient) getController() int {

}

func (this *ZkClient) getBrokerInfo(brokerId int) Broker {

}

func (this *ZkClient) getAllTopics() []string {

}

func (this *ZkClient) getAllPartitions() []TopicAndPartition {

}

func (this *ZkClient) getCluster() Cluster {
	getChildrenParentMayNotExist("/brokers/ids")
}

func (this *ZkClient) getReplicaAssignmentForTopics(topics []string) {

}

func (this *ZkClient) subscribeChildChanges(path string, handleChildChange func()) {

}

func (this *ZkClient) subscribeDataChanges(path string, handleDataChange func()) {

}

// session expires
func (this *ZkClient) subscribeStateChanges(func()) {

}

func (this *ZkClient) deletePath(path string) {

}
