package monitor

import (
	"encoding/json"
	"net/http"

	"github.com/funkygao/gafka"
	"github.com/funkygao/go-metrics"
	"github.com/julienschmidt/httprouter"
)

func (this *Monitor) setupRoutes() {
	this.router = httprouter.New()
	this.router.GET("/ver", this.versionHandler)
	this.router.GET("/metrics", this.metricsHandler)
}

// GET /metrics
func (this *Monitor) metricsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	b, err := json.Marshal(metrics.DefaultRegistry)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Write(b)
}

// GET /ver
func (this *Monitor) versionHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	w.Write([]byte(gafka.BuildId))
}
