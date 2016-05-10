package gateway

import (
	"net/http"

	log "github.com/funkygao/log4go"
)

func (this *Gateway) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	log.Error(r.RequestURI)
	this.writeBadRequest(w, "not found")
}
