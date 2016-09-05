package disk

import (
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
)

func (q *queue) pump() {
	defer func() {
		log.Trace("queue[%s] pump done", q.ident())
		q.wg.Done()
	}()

	var (
		b          block
		err        error
		okN, failN int
		retries    int
		backoff    time.Duration
	)
	for {
		select {
		case <-q.quit:
			log.Trace("queue[%s] flushed: %d/%d", q.ident(), okN, failN)
			return

		default:
		}

		backoff = initialBackoff

		err = q.Next(&b)
		switch err {
		case nil:
			q.emptyInflight = false

			for retries = 0; retries < defaultMaxRetries; retries++ {
				_, _, err = store.DefaultPubStore.SyncPub(q.clusterTopic.cluster, q.clusterTopic.topic, b.key, b.value)
				if err == nil {
					break
				}

				log.Debug("queue[%s] <%s>: %s", q.ident(), string(b.value), err)

				// backoff
				select {
				case <-q.quit:
					log.Trace("queue[%s] flushed: %d/%d", q.ident(), okN, failN)

					q.Rollback(&b)
					q.cursor.dump()
					return
				case <-time.After(backoff):
				}

				backoff *= 2
				if backoff >= maxBackoff {
					backoff = maxBackoff
				}
			}

			if err == nil {
				okN++
				continue
			}

			if err = q.Rollback(&b); err != nil {
				// should never happen
				log.Warn("queue[%s] skipped block <%s/%s>", q.ident(), string(b.key), string(b.value))

				failN++
			}

		case ErrQueueNotOpen:
			return

		case ErrEOQ:
			q.emptyInflight = true
			select {
			case <-q.quit:
				return
			case <-timer.After(pollEofSleep):
			}

		case ErrSegmentCorrupt:
			log.Error("queue[%s] pump: %s +%v", q.ident(), err, q.cursor.pos)
			q.skipCursorSegment()

		default:
			log.Error("queue[%s] pump: %s +%v", q.ident(), err, q.cursor.pos)
			q.skipCursorSegment()
		}
	}
}
