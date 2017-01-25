package agent

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (a *Agent) startServer(port int) {
	http.HandleFunc("/v1/members", a.members)

	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func (a *Agent) members(w http.ResponseWriter, r *http.Request) {
	b, _ := json.Marshal(a.Members())
	w.Write(b)
}
