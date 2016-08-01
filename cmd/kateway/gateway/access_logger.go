package gateway

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	log "github.com/funkygao/log4go"
)

// AccessLogger is a daily rotating/unblocking logger to record access log.
type AccessLogger struct {
	filename  string
	fd        *os.File
	lines     chan []byte
	discarded uint64
	stopped   chan struct{}
}

func NewAccessLogger(fn string, poolSize int) *AccessLogger {
	return &AccessLogger{
		filename: fn,
		lines:    make(chan []byte, poolSize),
		stopped:  make(chan struct{}),
	}
}

// Caution: NEVER call Log after Stop is called.
func (this *AccessLogger) Log(line []byte) {
	select {
	case this.lines <- line:
	default:
		// too busy, silently discard it
		total := atomic.AddUint64(&this.discarded, 1)
		if total%1000 == 0 {
			log.Warn("access logger discarded: %d", total)
		}
	}
}

func (this *AccessLogger) Start() error {
	var err error
	if this.fd, err = os.OpenFile(this.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660); err != nil {
		return err
	}

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()

		var lastDay int
		for {
			select {
			case t := <-tick.C:
				if this.fd != nil &&
					t.Day() != lastDay &&
					t.Hour() == 0 &&
					t.Minute() >= 0 &&
					t.Second() >= 0 {
					this.doRotate()
					lastDay = t.Day() // only once a day
				}

			case line, ok := <-this.lines:
				if !ok {
					// Stop() called, all inflight log lines flushed
					if this.fd != nil {
						this.fd.Close() // auto flush
						this.fd = nil
					}

					close(this.stopped)
					return
				}

				if this.fd != nil {
					this.fd.Write(line)
				}

			}
		}
	}()

	return nil
}

func (this *AccessLogger) doRotate() {
	var fname string
	_, err := os.Lstat(this.filename)
	if err == nil {
		// file exists, find a empty slot
		num := 1
		for ; err == nil && num <= 999; num++ {
			fname = this.filename + fmt.Sprintf(".%03d", num)
			_, err = os.Lstat(fname)
		}

		if err == nil {
			log.Error("Access logger unable to rotate, 30 years passed?")
			return
		}
	}

	this.fd.Close()
	this.fd = nil

	// if fd does not close, after rename, fd.Write happens
	// content will be written to new file
	err = os.Rename(this.filename, fname)
	if err != nil {
		log.Error("rename %s->%s: %v", this.filename, fname)
		return
	}

	if this.fd, err = os.OpenFile(this.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660); err != nil {
		log.Error("open(%s): %s", this.filename, err)
	}

}

func (this *AccessLogger) Stop() {
	close(this.lines)
	<-this.stopped
}

func (this *AccessLogger) Discarded() uint64 {
	return atomic.LoadUint64(&this.discarded)
}
