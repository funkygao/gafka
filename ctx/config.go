// Package ctx provides configurations loading and exporting.
package ctx

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"

	jsconf "github.com/funkygao/jsconf"
)

var (
	ErrInvalidZone = errors.New("Invalid zone")

	conf *config
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

func KafkaHome() string {
	ensureLogLoaded()
	return conf.kafkaHome
}

func InfluxdbHost() string {
	ensureLogLoaded()
	return conf.influxdbHost
}

func SortedZones() []string {
	ensureLogLoaded()
	return conf.sortedZones()
}

func ZoneZkAddrs(zone string) (zkAddrs string) {
	ensureLogLoaded()
	var present bool
	if zkAddrs, present = conf.zones[zone]; present {
		return
	}

	// should never happen
	panic(fmt.Errorf("zone[%s] undefined", zone))
}

type config struct {
	kafkaHome    string
	logLevel     string
	influxdbHost string
	zones        map[string]string // name:zkConn
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
	conf.influxdbHost = cf.String("influxdb_host", "")
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

func LoadFromHome() {
	const defaultConfig = `
{
    zones: [
        {
            name: "sit"
            zk: "10.77.144.87:10181,10.77.144.88:10181,10.77.144.89:10181"
        }
        {
            name: "test"
            zk: "10.77.144.101:10181,10.77.144.132:10181,10.77.144.182:10181"
        }
        {
            name: "pre"
            zk: ""
        }
        {
            name: "prod"
            zk: "10.209.33.69:2181,10.209.37.19:2181,10.209.37.68:2181"
        }
    ]

    kafka_home: "/opt/kafka_2.10-0.8.1.1"
    loglevel: "info"
}
`
	var configFile string
	if usr, err := user.Current(); err == nil {
		configFile = filepath.Join(usr.HomeDir, ".gafka.cf")
	} else {
		panic(err)
	}

	_, err := os.Stat(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			// create the config file on the fly
			if e := ioutil.WriteFile(configFile,
				[]byte(strings.TrimSpace(defaultConfig)), 0644); e != nil {
				panic(e)
			}
		} else {
			panic(err)
		}
	}

	LoadConfig(configFile)
}
