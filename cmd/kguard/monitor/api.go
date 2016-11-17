package monitor

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/funkygao/gafka"
	"github.com/funkygao/go-metrics"
	"github.com/julienschmidt/httprouter"
)

func (this *Monitor) setupRoutes() {
	this.router = httprouter.New()
	this.router.GET("/ver", this.versionHandler)
	this.router.GET("/metrics", this.metricsHandler)
}

// GET /metrics?debug=1
func (this *Monitor) metricsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	debug := r.URL.Query().Get("debug") == "1"
	if debug {
		b, err := json.Marshal(metrics.DefaultRegistry)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		w.Write(b)
		return
	}

	out := make(map[string]interface{}, 100)
	metrics.DefaultRegistry.Each(func(name string, i interface{}) {
		if strings.HasPrefix(name, "{") {
			// don't export the tag'ed metrics
			return
		}

		out[name] = i
	})
	b, err := json.Marshal(out)
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
