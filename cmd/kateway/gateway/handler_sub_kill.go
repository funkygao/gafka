package gateway

import (
	"net/http"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// DELETE /v1/conns
func (this *subServer) subKillHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	remoteAddr := r.Header.Get("X-Kill-Addr")
	if remoteAddr == "" {
		writeBadRequest(w, "nothing to kill")
		return
	}

	appid := r.Header.Get(HttpHeaderAppid)
	subkey := r.Header.Get(HttpHeaderSubkey)
	if !manager.Default.AuthAdmin(appid, subkey) {
		writeAuthFailure(w, manager.ErrAuthenticationFail)
		return
	}

	this.closedConnCh <- remoteAddr
	log.Info("killing sub conn: %s", remoteAddr)

	w.WriteHeader(http.StatusAccepted)
	w.Write(ResponseOk)
}
