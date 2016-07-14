package ctx

import (
	"github.com/funkygao/jsconf"
)

type zone struct {
	Name       string // prod
	Zk         string // localhost:2181,localhost:2182
	InfluxAddr string // localhost:8086
	SwfAddr    string // localhost:9195
}

func (this *zone) loadConfig(section *ljconf.Conf) {
	this.Name = section.String("name", "")
	this.Zk = section.String("zk", "")
	this.InfluxAddr = section.String("influxdb", "")
	this.SwfAddr = section.String("swf", "")
	if this.Name == "" {
		panic("empty zone name not allowed")
	}
}
