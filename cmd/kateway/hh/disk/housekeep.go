package disk

import (
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
)

func (l *queue) housekeeping() {
	defer func() {
		log.Trace("hh[%s] housekeeping quit", l.dir)
		l.wg.Done()
	}()

	purgeTick := time.NewTicker(l.purgeInterval)
	defer purgeTick.Stop()

	cursorChkpnt := time.NewTicker(time.Second)
	defer cursorChkpnt.Stop()

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
	defer func() {
		log.Trace("hh[%s] pump quit", l.dir)
		l.wg.Done()
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
