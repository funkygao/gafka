package mysql

import (
	"time"

	"github.com/funkygao/golib/idgen"
	log "github.com/funkygao/log4go"
)

func (this *mysqlStore) nextId() int64 {
	for {
		id, err := this.idgen.Next()
		if err != nil {
			if err == idgen.ErrorClockBackwards {
				log.Warn("%s, sleep 50ms", err)

				time.Sleep(time.Millisecond * 50)
				continue
			} else {
				// should never happen
				panic(err)
			}
		}

		return id
	}
}
