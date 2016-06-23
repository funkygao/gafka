// Package reporter perists github.com/funkygao/go-metrics
// metrics.Registry to durable storage.
package reporter

// A Reporter continously scans metrics.Registry and
// send all metrics to durable storage.
type Reporter interface {
	Name() string

	Start() error
	Stop()
}

var Default Reporter
