package disk

import (
	"time"

	log "github.com/funkygao/log4go"
)

func (q *queue) housekeeping() {
	defer func() {
		log.Trace("queue[%s] housekeeping done", q.ident())
		q.wg.Done()
	}()

	log.Trace("queue[%s] start housekeeping...", q.ident())

	purgeTick := time.NewTicker(q.purgeInterval)
	defer purgeTick.Stop()

	cursorChkpnt := time.NewTicker(time.Second)
	defer cursorChkpnt.Stop()

	for {
		select {
		case <-purgeTick.C:
			if err := q.Purge(); err != nil {
				log.Error("queue[%s] purge: %s", q.ident(), err)
			}

		case <-cursorChkpnt.C:
			if err := q.cursor.dump(); err != nil {
				log.Error("queue[%s] cursor checkpoint: %s", q.ident(), err)
			}

		case <-q.quit:
			return
		}
	}
}
