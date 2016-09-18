package monitor

import (
	"github.com/julienschmidt/httprouter"
)

func (this *Monitor) setupRoutes() {
	this.router = httprouter.New()
	this.router.GET("/ver", this.versionHandler)
	this.router.GET("/metrics", this.metricsHandler)
}
