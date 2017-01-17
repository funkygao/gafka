package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/gafka/telemetry/influxdb"
	"github.com/funkygao/go-metrics"
	gio "github.com/funkygao/golib/io"
)

var (
	z string
)

func init() {
	ctx.LoadFromHome()

	flag.StringVar(&z, "z", ctx.DefaultZone(), "zone")
	flag.Parse()
}

func main() {
	setupTelemetry()

	postLatency := metrics.NewRegisteredHistogram("influxdb.latency.post", nil, metrics.NewExpDecaySample(1028, 0.015))
	getLatency := metrics.NewRegisteredHistogram("influxdb.latency.get", nil, metrics.NewExpDecaySample(1028, 0.015))
	postSize := metrics.NewRegisteredHistogram("influxdb.size.post", nil, metrics.NewExpDecaySample(1028, 0.015))
	getSize := metrics.NewRegisteredHistogram("influxdb.size.get", nil, metrics.NewExpDecaySample(1028, 0.015))

	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := gio.ReadLine(reader)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				fmt.Println(err)
				continue
			}
		}

		pl, gl, ps, gs, err := parseLine(line)
		if err != nil {
			fmt.Println(err)
			continue
		}

		postLatency.Update(pl)
		getLatency.Update(gl)
		postSize.Update(ps)
		getSize.Update(gs)
	}

	fmt.Println("bye!")
}

func setupTelemetry() {
	zone := ctx.Zone(z)
	cf, err := influxdb.NewConfig(zone.InfluxAddr, "pubsub", "", "", time.Minute)
	if err != nil {
		panic(err)
	}

	telemetry.Default = influxdb.New(metrics.DefaultRegistry, cf)
	go telemetry.Default.Start()
}

// line e,g. [httpd] 10.213.1.223 - admin [11/Jan/2017:09:00:23 +0800] "GET /query?db=kfk_prod&epoch=ms&q=SELECT+sum%28%22m5%22%29+FROM+%22pub.qps.meter%22+WHERE+time+%3E+now%28%29+-+24h+GROUP+BY+time%281m%29+fill%28null%29%3B%0ASELECT+sum%28%22m5%22%29+FROM+%22consumer.qps.meter%22+WHERE+time+%3E+now%28%29+-+24h+GROUP+BY+time%281m%29+fill%28null%29 HTTP/1.1" 200 37369 "http://console.mycorp.com/dashboard/db/kafka-pub" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36" 54e84336-d799-11e6-a82d-000000000000 6369522
func parseLine(line []byte) (postLatency, getLatency, postSize, getSize int64, err error) {
	return
}
