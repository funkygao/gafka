// +build !fasthttp

package main

import (
	"net/http"

	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// POST /v1/ws/msgs/:topic/:ver
// TODO not implemented yet
func (this *Gateway) pubWsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		return
	}

	defer ws.Close()
}
