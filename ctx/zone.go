package ctx

import (
	"github.com/funkygao/jsconf"
)

type zone struct {
	Name        string // prod
	Zk          string // localhost:2181,localhost:2182
	InfluxAddr  string // localhost:8086
	SwfEndpoint string // http://192.168.10.134:9195/v1

	// smoke test related
	SmokeApp          string
	SmokeHisApp       string
	SmokeSecret       string
	SmokeTopic        string
	SmokeTopicVersion string
	SmokeGroup        string
}

func (this *zone) loadConfig(section *ljconf.Conf) {
	this.Name = section.String("name", "")
	this.Zk = section.String("zk", "")
	this.InfluxAddr = section.String("influxdb", "")
	this.SwfEndpoint = section.String("swf", "")
	this.SmokeApp = section.String("smoke_app", "")
	this.SmokeSecret = section.String("smoke_secret", "")
	this.SmokeTopic = section.String("smoke_topic", "smoketestonly")
	this.SmokeTopicVersion = section.String("smoke_topic_ver", "v1")
	this.SmokeHisApp = section.String("smoke_app_his", this.SmokeApp)
	this.SmokeGroup = section.String("smoke_group", "__smoketestonly__")
	if this.Name == "" {
		panic("empty zone name not allowed")
	}
}
