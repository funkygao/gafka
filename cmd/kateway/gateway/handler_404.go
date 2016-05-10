package gateway

import (
	"net/http"

	log "github.com/funkygao/log4go"
)

func (this *Gateway) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	log.Error(r.RequestURI)

	w.Header().Set("Connection", "close")
	this.writeErrorResponse(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}
