// Package ctx provides configurations loading and exporting.
package ctx

import (
	"errors"
	"sort"
)

var (
	ErrInvalidZone = errors.New("Invalid zone")

	conf *config
)

type config struct {
	hostname string // not config, but runtime

	kafkaHome    string
	logLevel     string
	influxdbHost string
	zones        map[string]string // name:zkConn
	tunnels      map[string]string // name:tunnel host ip
}

func (c *config) sortedZones() []string {
	sortedZones := make([]string, 0, len(c.zones))
	for name, _ := range c.zones {
		sortedZones = append(sortedZones, name)
	}
	sort.Strings(sortedZones)
	return sortedZones
}

func ensureLogLoaded() {
	if conf == nil {
		panic("call LoadConfig before this")
	}
}
