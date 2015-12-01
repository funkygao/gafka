package main

import (
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

	// TODO will create kafka topic on demand
	go this.waitExit()

}

func (this *executor) waitExit() {
	select {
	case <-this.gw.shutdownCh:
		log.Trace("executor stopped")
		this.gw.wg.Done()

	}
}
