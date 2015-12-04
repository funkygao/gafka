package main

import (
	"time"

	"github.com/funkygao/golib/window"
	log "github.com/funkygao/log4go"
	"github.com/shirou/gopsutil/cpu"
)

type guard struct {
	gw *Gateway

	refreshCh chan struct{}

	cpuStat cpu.CPUTimesStat

	win *window.MovingWindow
}

func newGuard(gw *Gateway) *guard {
	return &guard{
		gw:        gw,
		refreshCh: make(chan struct{}),
		win:       window.New(10, 1),
	}
}

func (this *guard) Start() {
	this.gw.wg.Add(1)
	defer this.gw.wg.Done()

	go this.mainLoop()
}

func (this *guard) mainLoop() {
	interval := time.Minute
	refreshTicker := time.NewTicker(interval)
	defer refreshTicker.Stop()
	alarmTicker := time.NewTicker(interval * 10)
	defer alarmTicker.Stop()

	refresh := func() {
		if v, err := cpu.CPUTimes(false); err == nil {
			this.cpuStat = v[0]
		}

		this.win.PushBack(this.cpuStat.User)
	}

	for {
		select {
		case <-this.gw.shutdownCh:
			log.Trace("guard stopped")
			return

		case <-this.refreshCh:
			refresh()

		case <-refreshTicker.C:
			refresh()

		case <-alarmTicker.C:
			loadTooHigh := true
			for _, load := range this.win.Slice() {
				if load < 0.9 {
					loadTooHigh = false
					break
				}
			}

			if loadTooHigh {
				// TODO in high load, should trigger elastic scaling event
				// send alarm email, sms
			}

		}
	}
}

func (this *guard) Refresh() {
	this.refreshCh <- struct{}{}
}
