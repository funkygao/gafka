// Package agent provides an HTTP endpoint for a program providing
// diagnostics and statistics for a given task.
package agent

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
)

var (
	HttpAddr = "localhost:10120"

	// Errors is the channel to receive errors of pprof agent.
	Errors = make(chan error, 1)
)

// Start starts the diagnostics agent on a host process. Once agent started,
// user can retrieve diagnostics via the HttpAddr endpoint.
func Start() (endpoint string) {
	// TODO access log
	go func() {
		if err := http.ListenAndServe(HttpAddr, nil); err != nil {
			Errors <- fmt.Errorf("pprof agent: %v", err)
		}
	}()

	return HttpAddr
}
