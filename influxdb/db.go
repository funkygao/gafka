package influxdb

import (
	"net/url"
	"time"

	"github.com/funkygao/gafka/config"
	"github.com/influxdb/influxdb/client/v2"
)

type Influxdb struct {
	conn client.Client
	bps  client.BatchPoints
}

func New() *Influxdb {
	url.Parse(config.InfluxdbHost())
	this := &Influxdb{}
	return this
}

func (this *Influxdb) Write(db string, name string, tags map[string]string,
	fields map[string]interface{}) error {
	p, e := client.NewPoint(name, tags, fields, time.Now())
	if e != nil {
		return e
	}
	this.bps.AddPoint(p)
	return nil
}

func (this *Influxdb) Start() {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for range ticker.C {
		this.conn.Write(this.bps)
	}
}
