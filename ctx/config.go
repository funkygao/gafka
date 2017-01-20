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
	hostname string // not by config, but runtime, cached value

	kafkaHome     string
	logLevel      string
	zkDefaultZone string // zk command default zone name
	upgradeCenter string
	zones         map[string]*zone // name:zone
	aliases       map[string]string
	reverseDns    map[string][]string // ip: domain names
}

func (c *config) sortedZones() []string {
	sortedZones := make([]string, 0, len(c.zones))
	for name := range c.zones {
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
