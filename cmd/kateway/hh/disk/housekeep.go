package disk

import (
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
)

func (q *queue) housekeeping() {
	defer func() {
		log.Trace("hh[%s] housekeeping quit", q.dir)
		q.wg.Done()
	}()

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
	defer func() {
		log.Trace("hh[%s] pump quit", q.dir)
		q.wg.Done()
	}()

	var (
		b   block
		err error
		i   int
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
			i++
			q.emptyInflight = false

			if false {
				//log.Info("%03d %s", i, string(b.value))
				log.Info("%04d", i)

				_, _, err = store.DefaultPubStore.SyncPub(q.clusterTopic.cluster, q.clusterTopic.topic, b.key, b.value)
				if err != nil {
					// TODO
					log.Error("{c:%s t:%s} %s", q.clusterTopic.cluster, q.clusterTopic.topic, err)
				}
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
