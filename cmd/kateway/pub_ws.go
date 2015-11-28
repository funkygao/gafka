package main

import (
	"net/http"

	log "github.com/funkygao/log4go"
)

func (this *Gateway) pubWsHandler(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		return
	}

	defer ws.Close()
}
