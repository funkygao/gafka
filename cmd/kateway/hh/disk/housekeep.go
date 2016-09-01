package disk

import (
	"io"
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

	go l.pump()

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

func (l *queue) pump() {
	var (
		b   block
		err error
		i   int
	)
	for {
		select {
		case <-l.quit:
			return

		default:
		}

		err = l.cursor.Next(&b)
		if err != nil {
			if err == io.EOF {
				l.emptyInflight = true
				time.Sleep(time.Second)
			} else {
				log.Error("hh pump: %s", err)
			}
			continue // FIXME return?
		}

		i++
		l.emptyInflight = false
		log.Info("%5d {c:%s t:%s} %s", i, l.cluster, l.topic, string(b.value))
		continue

		_, _, err = store.DefaultPubStore.SyncPub(l.cluster, l.topic, []byte(b.key), b.value)
		if err != nil {
			// TODO
			log.Error("{c:%s t:%s} %s", l.cluster, l.topic, err)
		}
	}
}
