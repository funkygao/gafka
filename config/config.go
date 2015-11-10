package config

import (
	"fmt"

	conf "github.com/funkygao/jsconf"
)

type Config struct {
	KafkaHome string
	Zones     map[string]string // name:zkConn
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
