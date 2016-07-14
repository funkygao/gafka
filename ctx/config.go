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

	kafkaHome       string
	logLevel        string
	consulBootstrap string            // consul bootstrap nodes addrs
	zones           map[string]string // zone:zkConn
	influxdbs       map[string]string // zone:influxdb addr
	zkDefaultZone   string            // zk command default zone name
	aliases         map[string]string
	reverseDns      map[string][]string // ip: domain names
	upgradeCenter   string
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
