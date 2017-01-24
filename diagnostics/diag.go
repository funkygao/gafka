package diagnostics

import (
	"net/http"
	_ "net/http/pprof"

	log "github.com/funkygao/log4go"
)

var HttpAddr = "localhost:10120"

// TODO access log
func Start() {
	go http.ListenAndServe(HttpAddr, nil)
	log.Info("pprof ready on %s", HttpAddr)
}
