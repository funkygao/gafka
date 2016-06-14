// Package metrics perists github.com/funkygao/go-metrics
// metrics.Registry to durable storage.
package metrics

// A Reporter continously scans metrics.Registry and
// send all metrics to durable storage.
type Reporter interface {
	Start() error
	Stop()
}

var DefaultReporter Reporter
