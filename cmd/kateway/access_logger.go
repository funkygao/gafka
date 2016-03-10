package main

import (
	"fmt"
	"os"
	"time"

	log "github.com/funkygao/log4go"
)

// AccessLogger is a daily rotating/unblocking logger to record access log.
type AccessLogger struct {
	filename string
	fd       *os.File

	lines  chan []byte
	rotate chan struct{}
}

func NewAccessLogger(fn string, poolSize int) (*AccessLogger, error) {
	this := &AccessLogger{filename: fn}
	this.lines = make(chan []byte, poolSize)
	this.rotate = make(chan struct{})
	var err error
	if this.fd, err = os.OpenFile(fn, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660); err != nil {
		return nil, err
	}

	return this, nil
}

func (this *AccessLogger) Log(line []byte) {
	select {
	case this.lines <- line:
	default:
		// too busy, silently discard it
	}
}

func (this *AccessLogger) Start() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	var lastDay int
	for {
		select {
		case t := <-tick.C:
			if t.Day() > lastDay &&
				t.Hour() == 0 &&
				t.Minute() >= 0 &&
				t.Second() >= 0 {
				this.doRotate()
				lastDay = t.Day() // only once a day
			}

		case line, ok := <-this.lines:
			// FIXME a 00:00:00, might lose access log
			if !ok {
				// stopped
				return
			}

			this.fd.Write(line)
		}
	}

}

func (this *AccessLogger) doRotate() {
	_, err := os.Lstat(this.filename)
	if err == nil {
		// file exists, find a empty slot
		num := 1
		fname := ""
		for ; err == nil && num <= 999; num++ {
			fname = this.filename + fmt.Sprintf(".%03d", num)
			_, err = os.Lstat(fname)
		}

		if err == nil {
			log.Error("Access logger unable to rotate, 30 years passed?")
			return
		}

		// if after rename, this.fd.Write happens, content will be written to new file
		err = os.Rename(this.filename, fname)
		if err != nil {
			log.Error("rename %s->%s: %v", this.filename, fname)
			return
		}
	}

	fd, err := os.OpenFile(this.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Error("open(%s): %s", this.filename, err)
		return
	}

	this.fd = fd
}

func (this *AccessLogger) Stop() {
	close(this.lines)
	if this.fd != nil {
		this.fd.Close()
	}
}
