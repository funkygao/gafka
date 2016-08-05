package external

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	//"github.com/funkygao/go-metrics"
	//"github.com/fsnotify/fsnotify"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("external.exec", func() monitor.Watcher {
		return &WatchExec{}
	})
}

// WatchExec watches external scripts stdout and feeds into influxdb.
type WatchExec struct {
	Stop <-chan struct{}
	Wg   *sync.WaitGroup

	confDir string
}

func (this *WatchExec) Init(ctx monitor.Context) {
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
	this.confDir = ctx.ExternalDir()
}

func (this *WatchExec) Run() {
	defer this.Wg.Done()

	if this.confDir == "" {
		log.Warn("empty confd, external.exec disabled")
		return
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	if err := this.watchConfigDir(); err != nil {
		log.Error("%v", err)
		return
	}

	for {
		select {
		case <-this.Stop:
			log.Info("external.exec stopped")
			return

		case <-ticker.C:

		}
	}
}

func (this *WatchExec) watchConfigDir() error {
	/*
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			return err
		}

		err = watcher.Add(this.confDir)
		if err != nil {
			return err
		}

		go func() {
			for {
				select {
				case <-this.Stop:
					watcher.Close()
					return

				case err := <-watcher.Errors:
					log.Error("inotify %s: %v", this.confDir, err)

				case event := <-watcher.Events:
					if event.Op&fsnotify.Write == fsnotify.Write {
						// file modified
					}
					if event.Op&fsnotify.Remove == fsnotify.Remove {
						// file deleted
					}
					if event.Op&fsnotify.Create == fsnotify.Create {
						// file added
					}
				}
			}
		}()*/

	return nil
}
