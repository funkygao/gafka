package monitor

import (
	"sync"

	"github.com/funkygao/gafka/zk"
)

// Context is the context container that will be passed to plugin watchers.
type Context interface {
	ZkZone() *zk.ZkZone
	StopChan() <-chan struct{}
	WaitGroup() *sync.WaitGroup
	InfluxAddr() string
	InfluxDB() string
}
