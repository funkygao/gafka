package main

import (
	"time"

	"github.com/funkygao/gafka/cmd/pubsub/command"
	"github.com/funkygao/log4go"
)

// TODO how to load balance across the cluster?
func main() {
	level := log4go.DEBUG // TODO
	for _, filter := range log4go.Global {
		filter.Level = level
	}

	log4go.AddFilter("stdout", level, log4go.NewConsoleLogWriter())

	zk := command.NewZk(command.DefaultConfig("", command.ZkAddr))
	for {
		_, _, events, err := zk.Conn().GetW(command.BindChange)
		if err != nil {
			panic(err)
		}

		select {
		case <-time.After(time.Minute):
			log4go.Debug("no binding change during the last 1m")

		case <-events:
			log4go.Info("bindings changed, dispatching to workers")
			dispatchWorkers(zk)
		}

	}

}
