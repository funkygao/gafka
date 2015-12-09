package main

type dispatcher struct {
}

func newDispatcher() *dispatcher {
	return &dispatcher{}
}

func (this *dispatcher) lookup(appid, topic string) (cluster string) {
	cluster = options.cluster
	return
}
