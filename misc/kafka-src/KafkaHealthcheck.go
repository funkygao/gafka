package main

type KafkaHealthcheck struct {
	zkClient *ZkClient
}

func (this *KafkaHealthcheck) Startup() {
	this.zkClient.subscribeStateChanges(func() {
		this.register()
	})
}

func (this *KafkaHealthcheck) register() {
	this.zkClient.registerBrokerInZk()
}
