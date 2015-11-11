package config

import (
	"errors"
	"fmt"
	"sort"

	conf "github.com/funkygao/jsconf"
)

var (
	ErrInvalidZone = errors.New("Invalid zone")
)

type Config struct {
	KafkaHome string
	Zones     map[string]string // name:zkConn
}

func (c *Config) ZkAddrs(zone string) (string, error) {
	if zkAddrs, present := c.Zones[zone]; present {
		return zkAddrs, nil
	}

	return "", ErrInvalidZone
}

func (c *Config) SortedZones() []string {
	zones := make([]string, len(c.Zones))
	idx := 0
	for name, _ := range c.Zones {
		zones[idx] = name
		idx++
	}
	sort.Strings(zones)
	return zones
}

func LoadConfig(fn string) *Config {
	cf, err := conf.Load(fn)
	if err != nil {
		panic(err)
	}

	this := new(Config)
	this.KafkaHome = cf.String("kafka_home", "")
	this.Zones = make(map[string]string)
	for i := 0; i < len(cf.List("zones", nil)); i++ {
		section, err := cf.Section(fmt.Sprintf("zones[%d]", i))
		if err != nil {
			panic(err)
		}

		z := new(zone)
		z.loadConfig(section)
		this.Zones[z.name] = z.zk
	}

	return this
}
