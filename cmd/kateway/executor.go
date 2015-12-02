package main

import (
	"github.com/funkygao/gafka/cmd/kateway/api"
	log "github.com/funkygao/log4go"
	"github.com/wvanbergen/kafka/consumergroup"
)

// Subscribe to internal topics and execute commands on command event arrives.
type executor struct {
	gw *Gateway

	cg *consumergroup.ConsumerGroup
}

func newExecutor(gw *Gateway) *executor {
	this := &executor{
		gw: gw,
	}
	return this
}

func (this *executor) Start() {
	this.gw.wg.Add(1)

	go this.waitExit()
	go this.addTopicLoop()
}

func (this *executor) addTopicLoop() {
	c := api.NewClient(nil)
	c.Connect("http://localhost:9192")
	c.Subscribe("v1", "_addtopic", "_kateway", func(cmd []byte) (err error) {
		return
	})
}

func (this *executor) waitExit() {
	select {
	case <-this.gw.shutdownCh:
		log.Trace("executor stopped")
		this.gw.wg.Done()

	}
}
