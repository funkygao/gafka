package disk

import (
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
)

func (q *queue) housekeeping() {
	defer q.wg.Done()

	purgeTick := time.NewTicker(q.purgeInterval)
	defer purgeTick.Stop()

	cursorChkpnt := time.NewTicker(time.Second)
	defer cursorChkpnt.Stop()

	for {
		select {
		case <-purgeTick.C:
			if err := q.Purge(); err != nil {
				log.Error("hh purge: %s", err)
			}

		case <-cursorChkpnt.C:
			if err := q.cursor.dump(); err != nil {
				log.Error("hh cursor checkpoint: %s", err)
			}

		case <-q.quit:
			return
		}
	}
}

func (q *queue) pump() {
	defer q.wg.Done()

	var (
		b       block
		err     error
		n       int
		retries int
	)
	for {
		select {
		case <-q.quit:
			return

		default:
		}

		err = q.Next(&b)
		switch err {
		case nil:
			q.emptyInflight = false

			if store.DefaultPubStore != nil {
				_, _, err = store.DefaultPubStore.SyncPub(q.clusterTopic.cluster, q.clusterTopic.topic, b.key, b.value)
			} else {
				err = ErrNoUnderlying
			}
			if err != nil {
				if retries >= maxRetries {
					retries = 0
					n++
				} else {
					retries++
					log.Error("queue[%s] %d/#%d %s: %s", q.ident(), n+1, retries, err, string(b.value))

					if err = q.Rollback(&b); err != nil {
						log.Error("queue[%s] %d/#%d %s: %s", q.ident(), n+1, retries, err, string(b.value))

						retries = 0
						n++
						continue
					}

					time.Sleep(backoffDuration)
				}
			} else {
				n++
			}

		case ErrQueueNotOpen:
			return

		case ErrEOQ:
			q.emptyInflight = true
			time.Sleep(time.Second)

		case ErrSegmentCorrupt:
			log.Error("queue[%s] pump: %s +%v", q.ident(), err, q.cursor.pos)
			q.skipCursorSegment()

		default:
			log.Error("queue[%s] pump: %s +%v", q.ident(), err, q.cursor.pos)
			q.skipCursorSegment()
		}
	}
}
