package agent

import (
	"encoding/json"
	"fmt"
	"net/http"

	log "github.com/funkygao/log4go"
)

func (a *Agent) startAPIServer(port int) {
	http.HandleFunc("/v1/state", a.stateHandler)
	http.HandleFunc("/v1/members", a.membersHandler)

	// FIXME security
	addr := fmt.Sprintf(":%d", port)
	log.Info("api server ready on %s", addr)
	http.ListenAndServe(addr, nil)
}

func (a *Agent) stateHandler(w http.ResponseWriter, r *http.Request) {
	b, _ := json.Marshal(a.State())
	w.Write(b)
}

func (a *Agent) membersHandler(w http.ResponseWriter, r *http.Request) {
	b, _ := json.Marshal(a.State()["members"])
	w.Write(b)
}

func (a *Agent) membersUri(port int) string {
	return fmt.Sprintf("http://localhost:%d/v1/members", apiPort(port))
}
