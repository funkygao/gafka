package sos

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/funkygao/gafka/sos"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

var (
	sosMetrics  = metrics.NewRegisteredCounter("sos", nil)
	lastSos     time.Time
	IdleTimeout = 5 * time.Minute
)

func init() {
	http.HandleFunc("/", handleSOS)
	go http.ListenAndServe(fmt.Sprintf(":%d", sos.SOSPort), nil)
	go maintainSosCounter()

	log.Info("SOS receiver started")
}

func handleSOS(w http.ResponseWriter, r *http.Request) {
	sosMsg, _ := ioutil.ReadAll(r.Body)
	r.Body.Close()

	sosMetrics.Inc(1)
	lastSos = time.Now()
	log.Critical("SOS[%s] from %s %s", r.Header.Get(sos.IdentHeader), r.RemoteAddr, string(sosMsg))

	w.WriteHeader(http.StatusAccepted)
}

func maintainSosCounter() {
	for {
		time.Sleep(time.Minute)

		if time.Since(lastSos) >= IdleTimeout {
			if sosMetrics.Count() > 0 {
				log.Info("SOS[#%d] idle over %s, metrics reset", sosMetrics.Count(), IdleTimeout)
			}
			sosMetrics.Clear()
		}
	}
}
