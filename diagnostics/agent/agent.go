// Package agent provides an HTTP endpoint for a program providing
// diagnostics and statistics for a given task.
package agent

import (
	"net/http"
	_ "net/http/pprof"
)

var HttpAddr = "localhost:10120"

// Start starts the diagnostics agent on a host process. Once agent started,
// user can retrieve diagnostics via the HttpAddr endpoint.
func Start() (endpoint string) {
	// TODO access log
	go http.ListenAndServe(HttpAddr, nil)
	return HttpAddr
}
