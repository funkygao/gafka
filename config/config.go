package config

import (
	"errors"
	"fmt"
	"sort"

	jsconf "github.com/funkygao/jsconf"
)

var (
	ErrInvalidZone = errors.New("Invalid zone")
)

func ensureLogLoaded() {
	if conf == nil {
		panic("call LoadConfig before this")
	}
}

func LogLevel() string {
	ensureLogLoaded()
	return conf.logLevel
}

func Zones() map[string]string {
	ensureLogLoaded()
	return conf.zones
}

func SortedZones() []string {
	ensureLogLoaded()
	return conf.sortedZones()
}

func ZonePath(zone string) (zkAddrs string) {
	ensureLogLoaded()
	var present bool
	if zkAddrs, present = conf.zones[zone]; present {
		return
	}

	// should never happen
	panic(zone + " undefined")
}

type config struct {
	kafkaHome string
	logLevel  string
	zones     map[string]string // name:zkConn
}

func (c *config) sortedZones() []string {
	sortedZones := make([]string, 0, len(c.zones))
	for name, _ := range c.zones {
		sortedZones = append(sortedZones, name)
	}
	sort.Strings(sortedZones)
	return sortedZones
}

func LoadConfig(fn string) {
	cf, err := jsconf.Load(fn)
	if err != nil {
		panic(err)
	}

	conf = new(config)
	conf.kafkaHome = cf.String("kafka_home", "")
	conf.logLevel = cf.String("loglevel", "info")
	conf.zones = make(map[string]string)
	for i := 0; i < len(cf.List("zones", nil)); i++ {
		section, err := cf.Section(fmt.Sprintf("zones[%d]", i))
		if err != nil {
			panic(err)
		}

		z := new(zone)
		z.loadConfig(section)
		conf.zones[z.name] = z.zk
	}

}
