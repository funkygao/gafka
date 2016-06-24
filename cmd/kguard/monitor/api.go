package monitor

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/rcrowley/go-metrics"
)

// /metrics
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
