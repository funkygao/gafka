package agent

import (
	"time"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/manager/mysql"
	log "github.com/funkygao/log4go"
)

func (this *Agent) runManager() {
	cf := mysql.DefaultConfig(this.zone)
	cf.Refresh = time.Minute * 5
	manager.Default = mysql.New(cf)
	if err := manager.Default.Start(); err != nil {
		panic(err)
	}

	log.Trace("manager store[%s] started", manager.Default.Name())
}
