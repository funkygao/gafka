package main

// /config/changes
// /brokers/topics/<topic_name>/config
type TopicConfigManager struct {
	zkClient *ZkClient
}

func (this *TopicConfigManager) Startup() {
	this.zkClient.subscribeChildChanges("/config/changes", func(path string) {
		this.processAllConfigChanges(path)
	})

	this.processAllConfigChanges("/config/changes")
}

func (this *TopicConfigManager) processAllConfigChanges(path string) {

}
