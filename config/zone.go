package config

import (
	conf "github.com/funkygao/jsconf"
)

type zone struct {
	name string
	zk   string
}

func (this *zone) loadConfig(section *conf.Conf) {
	this.name = section.String("name", "")
	this.zk = section.String("zk", "")
	if this.name == "" || this.zk == "" {
		panic("empty zone not allowed")
	}
}
