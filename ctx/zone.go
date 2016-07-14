package ctx

import (
	"github.com/funkygao/jsconf"
)

type zone struct {
	name       string
	zk         string
	influxAddr string // 10.1.1.1:8086
}

func (this *zone) loadConfig(section *ljconf.Conf) {
	this.name = section.String("name", "")
	this.zk = section.String("zk", "")
	this.influxAddr = section.String("influxdb", "")
	if this.name == "" {
		panic("empty zone name not allowed")
	}
}
