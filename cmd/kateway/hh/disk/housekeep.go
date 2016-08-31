package disk

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
)

func (l *queue) housekeeping(purgeInterval time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	tick := time.NewTicker(purgeInterval)
	defer tick.Stop()

	go l.pump()

	purgeCh := tick.C
	if l.maxAge == 0 {
		purgeCh = nil
	}

	for {
		select {
		case now := <-purgeCh:
			if err := l.PurgeOlderThan(now.Add(l.maxAge)); err != nil {
				log.Error("hh purge: %s", err)
			}

		case <-l.quit:
			return
		}
	}
}

func (l *queue) pump() {
	var (
		b   block
		err error
	)
	for {
		select {
		case <-l.quit:
			return

		default:
		}

		err = l.cursor.Next(&b)
		if err != nil {
			log.Error("hh pump: %s", err)
			continue // FIXME return?
		}

		store.DefaultPubStore.SyncPub(l.cluster, l.topic, []byte(b.key), b.value)
	}
}
