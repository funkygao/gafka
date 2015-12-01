package main

import (
	"syscall"
	"time"
)

type guard struct {
	gw *Gateway

	usrCpuUtil float64
	sysCpuUtil float64
}

func newGuard(gw *Gateway) *guard {
	return &guard{
		gw: gw,
	}
}

func (this *guard) Start() {
	interval := time.Minute
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var (
		rusage       = &syscall.Rusage{}
		lastUserTime int64
		lastSysTime  int64
		userTime     int64
		sysTime      int64
	)
	for {
		select {
		case <-this.gw.shutdownCh:
			return

		case <-ticker.C:
			syscall.Getrusage(syscall.RUSAGE_SELF, rusage)
			syscall.Getrusage(syscall.RUSAGE_SELF, rusage)
			userTime = rusage.Utime.Sec*1000000000 + int64(rusage.Utime.Usec)
			sysTime = rusage.Stime.Sec*1000000000 + int64(rusage.Stime.Usec)
			this.usrCpuUtil = float64(userTime-lastUserTime) * 100 / float64(interval)
			this.sysCpuUtil = float64(sysTime-lastSysTime) * 100 / float64(interval)

			lastUserTime = userTime
			lastSysTime = sysTime

			// TODO in high load, should trigger elastic scaling event
		}
	}

}

func (this *guard) CpuUsage() (float64, float64) {
	return this.usrCpuUtil, this.sysCpuUtil
}
