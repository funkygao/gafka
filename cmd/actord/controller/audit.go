package controller

import (
	"os"

	log "github.com/funkygao/log4go"
)

func (this *controller) setupAuditor() {
	this.auditor = log.NewDefaultLogger(log.TRACE)
	this.auditor.DeleteFilter("stdout")

	_ = os.Mkdir("audit", os.ModePerm)
	rotateEnabled, discardWhenDiskFull := true, false
	filer := log.NewFileLogWriter("audit/actord.log", rotateEnabled, discardWhenDiskFull, 0644)
	if filer == nil {
		panic("failed to open audit log")
	}
	filer.SetFormat("[%d %T] [%L] (%S) %M")
	filer.SetRotateLines(0)
	filer.SetRotateDaily(true)
	this.auditor.AddFilter("file", log.TRACE, filer)
}
