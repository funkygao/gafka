package ctx

import (
	"github.com/funkygao/jsconf"
)

type zone struct {
	Name        string // prod
	Zk          string // localhost:2181,localhost:2182
	InfluxAddr  string // localhost:8086
	SwfEndpoint string // http://192.168.10.134:9195/v1

	ZkHelix string // localhost:2181/helix

	// smoke test related
	PubEndpoint, SubEndpoint string // the load balancer addr
	SmokeApp                 string
	SmokeHisApp              string
	SmokeSecret              string
	SmokeTopic               string
	SmokeTopicVersion        string
	SmokeGroup               string
	HaProxyStatsUri          []string

	// zk digest user:passwd
	ZkAuth string

	GkAuditCluster, GkAuditTopic string

	AdminUser, AdminPass string
}

func (this *zone) loadConfig(section *ljconf.Conf) {
	this.Name = section.String("name", "")
	if this.Name == "" {
		panic("empty zone name not allowed")
	}
	this.Zk = section.String("zk", "")
	this.ZkAuth = section.String("zk_auth", "")
	this.ZkHelix = section.String("zk_helix", "")
	this.AdminUser = section.String("admin_user", "_psubAdmin_")
	this.AdminPass = section.String("admin_pass", "_wandafFan_")
	this.InfluxAddr = section.String("influxdb", "")
	this.SwfEndpoint = section.String("swf", "")
	this.PubEndpoint = section.String("pub_entry", "")
	this.SubEndpoint = section.String("sub_entry", "")
	this.SmokeApp = section.String("smoke_app", "")
	this.SmokeSecret = section.String("smoke_secret", "")
	this.SmokeTopic = section.String("smoke_topic", "smoketestonly")
	this.SmokeTopicVersion = section.String("smoke_topic_ver", "v1")
	this.SmokeHisApp = section.String("smoke_app_his", this.SmokeApp)
	this.SmokeGroup = section.String("smoke_group", "__smoketestonly__")
	this.HaProxyStatsUri = section.StringList("haproxy_stats", nil)
	this.GkAuditCluster = section.String("gkaudit_cluster", "")
	this.GkAuditTopic = section.String("gkaudit_topic", "gk")
}
