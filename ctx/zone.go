package ctx

import (
	"github.com/funkygao/jsconf"
)

type zone struct {
	name   string
	zk     string
	tunnel string
}

func (this *zone) loadConfig(section *ljconf.Conf) {
	this.name = section.String("name", "")
	this.zk = section.String("zk", "")
	this.tunnel = section.String("tunnel", "")
	if this.name == "" {
		panic("empty zone name not allowed")
	}
}
