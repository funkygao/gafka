package disk

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
)

func (q *queue) FlushInflights(errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	var (
		b       block
		err     error
		n       int
		backoff = initialBackoff
	)
	for {
		backoff = initialBackoff
		err = q.Next(&b)
		switch err {
		case nil:
			for retries := 0; retries < 5; retries++ {
				_, _, err = store.DefaultPubStore.SyncPub(q.clusterTopic.cluster, q.clusterTopic.topic, b.key, b.value)
				if err == nil {
					break
				} else {
					log.Debug("queue[%s] <%s>: %s", q.ident(), string(b.value), err)

					time.Sleep(backoff)
					backoff *= 2
					if backoff >= maxBackoff {
						backoff = maxBackoff
					}
				}
			}

			if err == nil {
				q.cursor.dump() // checkpoint
				n++
				continue
			}

			errCh <- err

			if err = q.Rollback(&b); err != nil {
				// should never happen
				log.Error("queue[%s] %d %s: %s", q.ident(), n+1, err, string(b.value))
				errCh <- err
			}
			return

		case ErrQueueNotOpen:
			errCh <- err
			return

		case ErrEOQ:
			log.Debug("queue[%s] flushed %d inflights", q.ident(), n)
			return

		case ErrSegmentCorrupt:
			q.skipCursorSegment()
			errCh <- err

		default:
			q.skipCursorSegment()
			errCh <- err
		}
	}
}
