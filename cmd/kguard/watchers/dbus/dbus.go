package dbus

import (
	"fmt"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("dbus.dbus", func() monitor.Watcher {
		return &WatchDbus{}
	})
}

// WatchDbus watches databus aliveness.
type WatchDbus struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Wg     *sync.WaitGroup
}

func (this *WatchDbus) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchDbus) Run() {
	defer this.Wg.Done()

	dbs := metrics.NewRegisteredGauge("dbus.myslave.db", nil)
	owner := metrics.NewRegisteredGauge("dbus.myslave.owenr", nil)
	runner := metrics.NewRegisteredGauge("dbus.myslave.runner", nil)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-this.Stop:
			log.Info("dbus.dbus stopped")
			return

		case <-ticker.C:
			dbN, ownerN, runnerN := this.fetchData()
			dbs.Update(int64(dbN))
			owner.Update(int64(ownerN))
			runner.Update(int64(runnerN))

		}
	}
}

func (this *WatchDbus) fetchData() (dbN int, ownerN int, runnerN int) {
	root := "/dbus/myslave"
	dbs, _, err := this.Zkzone.Conn().Children(root)
	if err != nil {
		log.Error("%s %s", root, err)
		return
	}
	dbN = len(dbs)

	for _, db := range dbs {
		idsPath := fmt.Sprintf("%s/%s/ids", root, db)
		runners, _, err := this.Zkzone.Conn().Children(idsPath)
		if err != nil {
			log.Error("%s %s", idsPath, err)
			continue
		}
		runnerN += len(runners)

		ownerPath := fmt.Sprintf("%s/%s/owner", root, db)
		data, _, err := this.Zkzone.Conn().Get(ownerPath)
		if err != nil {
			log.Error("%s %s", ownerPath, err)
		} else if len(data) == 0 {
			log.Error("empty master: %s", ownerPath)
		} else {
			ownerN += 1
		}
	}

	return
}
