package disk

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
)

func (l *queue) housekeeping(purgeInterval time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	purgeTick := time.NewTicker(purgeInterval)
	defer purgeTick.Stop()

	cursorChkpnt := time.NewTicker(time.Second)
	defer cursorChkpnt.Stop()

	wg.Add(1)
	go l.pump(wg)

	for {
		select {
		case <-purgeTick.C:
			if err := l.Purge(); err != nil {
				log.Error("hh purge: %s", err)
			}

		case <-cursorChkpnt.C:
			if err := l.cursor.dump(); err != nil {
				log.Error("hh cursor checkpoint: %s", err)
			}

		case <-l.quit:
			return
		}
	}
}

func (l *queue) pump(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		log.Trace("hh pump quit")
	}()

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

		err = l.Next(&b)
		switch err {
		case nil:
			l.emptyInflight = false
			log.Info("%s", string(b.value))

		case ErrNotOpen:
			return

		case ErrEOQ:
			l.emptyInflight = true
			time.Sleep(time.Second)

		default:
			log.Error("hh pump: %s +%v", err, l.cursor.pos)
		}

		continue

		_, _, err = store.DefaultPubStore.SyncPub(l.clusterTopic.cluster, l.clusterTopic.topic, b.key, b.value)
		if err != nil {
			// TODO
			log.Error("{c:%s t:%s} %s", l.clusterTopic.cluster, l.clusterTopic.topic, err)
		}
	}
}
