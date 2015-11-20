package ctx

import (
	"github.com/funkygao/jsconf"
)

type zone struct {
	name string
	zk   string
}

func (this *zone) loadConfig(section *ljconf.Conf) {
	this.name = section.String("name", "")
	this.zk = section.String("zk", "")
	if this.name == "" {
		panic("empty zone name not allowed")
	}
}
