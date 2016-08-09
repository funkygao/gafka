package controller

import (
	"net/http"

	log "github.com/funkygao/log4go"
)

func (this *controller) runWebServer() {
	http.HandleFunc("/v1/status", this.statusHandler)
	log.Info("web server on %s ready", this.ListenAddr)
	err := http.ListenAndServe(this.ListenAddr, nil)
	if err != nil {
		log.Error("ListenAndServe: ", err)
	}
}

func (this *controller) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf8")
	w.Write(this.Bytes())
}
