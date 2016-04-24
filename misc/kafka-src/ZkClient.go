package main

type ZkClient struct {
}

func initZk() *ZkClient {
	// setup common znode paths
	for path := range []string{ConsumersPath, BrokerIdsPath, BrokerTopicsPath, TopicConfigChangesPath, TopicConfigPath, DeleteTopicsPath} {
		makeSurePersistentPathExists(path)
	}
	return &ZkClient{}
}

func (this *ZkClient) FetchAllTopicConfigs() {

}
