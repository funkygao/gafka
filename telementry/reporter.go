// Package telementry perists github.com/funkygao/go-metrics
// metrics.Registry to durable storage.
package telementry

// A Reporter continously scans metrics.Registry and
// persists all metrics to durable storage.
type Reporter interface {
	Name() string

	Start() error
	Stop()
}

var Default Reporter
