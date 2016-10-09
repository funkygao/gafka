package command

import (
	"encoding/json"
	"net/http"

	"github.com/funkygao/gafka"
	log "github.com/funkygao/log4go"
)

func runMonitorServer(addr string) {
	http.HandleFunc("/v1/status", statusHandler)
	log.Info("web server on %s ready", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Error("ListenAndServe: %s", err)
	}
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf8")
	w.Header().Set("Server", "ehaproxy")

	v := map[string]string{
		"version": gafka.BuildId,
	}
	b, _ := json.Marshal(v)
	w.Write(b)
}
