package monitor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/funkygao/gafka"
	"github.com/funkygao/go-metrics"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

func (this *Monitor) setupRoutes() {
	this.router = httprouter.New()
	this.router.GET("/ver", this.versionHandler)
	this.router.GET("/metrics", this.metricsHandler)
	this.router.PUT("/set", this.configHandler)
	this.router.POST("/alertHook", this.alertHookHandler) // zabbix will call me on alert event
	this.router.POST("/lags", this.cgLagsHandler)
	this.router.POST("/kfk/clusters", this.kfkClustersHandler)
	this.router.POST("/kfk/topics", this.kfkTopicsHandler)
	this.router.POST("/kfk/topicsGroups", this.kfkTopicsGroupsHandler)
	this.router.GET("/zk/cluster", this.zkClusterHandler)
}

// PUT /set?key=xx
func (this *Monitor) configHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	key := r.URL.Query().Get("key")
	n := 0
	for _, w := range this.watchers {
		if s, ok := w.(Setter); ok {
			s.Set(key)
			n++
		}
	}

	log.Info("key[%s] %d watchers applied", key, n)
	w.Write([]byte(fmt.Sprintf("%d watchers applied", n)))
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

	data := make(map[string]map[string]interface{})
	metrics.DefaultRegistry.Each(func(name string, i interface{}) {
		if strings.HasPrefix(name, "{") {
			// don't export the tag'ed metrics
			// TODO ignore _XXX?
			return
		}

		values := make(map[string]interface{})
		switch metric := i.(type) {
		case metrics.Counter:
			values["count"] = metric.Count()
		case metrics.Gauge:
			values["value"] = metric.Value()
		case metrics.GaugeFloat64:
			values["value"] = metric.Value()
		case metrics.Healthcheck:
			values["error"] = nil
			metric.Check()
			if err := metric.Error(); nil != err {
				values["error"] = metric.Error().Error()
			}
		case metrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			values["count"] = h.Count()
			values["min"] = h.Min()
			values["max"] = h.Max()
			values["mean"] = h.Mean()
			values["stddev"] = h.StdDev()
			values["median"] = ps[0]
			values["75%"] = ps[1]
			values["95%"] = ps[2]
			values["99%"] = ps[3]
			values["99.9%"] = ps[4]
		case metrics.Meter:
			m := metric.Snapshot()
			values["count"] = m.Count()
			values["1m.rate"] = m.Rate1()
			values["5m.rate"] = m.Rate5()
			values["15m.rate"] = m.Rate15()
			values["mean.rate"] = m.RateMean()
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			values["count"] = t.Count()
			values["min"] = t.Min()
			values["max"] = t.Max()
			values["mean"] = t.Mean()
			values["stddev"] = t.StdDev()
			values["median"] = ps[0]
			values["75%"] = ps[1]
			values["95%"] = ps[2]
			values["99%"] = ps[3]
			values["99.9%"] = ps[4]
			values["1m.rate"] = t.Rate1()
			values["5m.rate"] = t.Rate5()
			values["15m.rate"] = t.Rate15()
			values["mean.rate"] = t.RateMean()
		}
		data[name] = values
	})
	b, err := json.Marshal(data)
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
	w.Header().Set("Content-Type", "application/json; charset=utf8")
	w.Header().Set("Server", "kguard")

	log.Info("%s ver", r.RemoteAddr)

	v := map[string]string{
		"version": gafka.BuildId,
		"uptime":  strconv.FormatInt(this.startedAt.Unix(), 10),
		"lead":    strconv.FormatInt(this.leadAt.Unix(), 10),
	}
	b, _ := json.Marshal(v)
	w.Write(b)
}
