package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime/debug"
	"syscall"
	"time"

	"strconv"

	"strings"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/gafka/telemetry/influxdb"
	"github.com/funkygao/go-metrics"
	gio "github.com/funkygao/golib/io"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
	"github.com/satyrius/gonx"
)

var (
	z          string
	v          bool
	logfile    string
	parser     *gonx.Parser
	httpdRegex = regexp.MustCompile(`^\[httpd\]`) //log begin with [httpd]

	ErrInvalidLogFormatLine = errors.New("invalid log format line")
)

const (
	defaultLogfile = "influxwatch.log"
)

func init() {
	ctx.LoadFromHome()

	flag.StringVar(&z, "z", ctx.DefaultZone(), "zone")
	flag.BoolVar(&v, "version", false, "version")
	flag.StringVar(&logfile, "log", defaultLogfile, "")
	flag.Parse()

	//init log parser
	httpdFormat := "[$prefix] $remote_addr $remote_log_name $remote_user [$start_time] \"$request\" $status $resp_bytes \"$referer\" \"$user_agent\" $req_id $time_elapsed"
	parser = gonx.NewParser(httpdFormat)
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	if v {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	//config log
	setupLogging(logfile, "trace", "panic")

	log.Info("influxwatch[%s] starting...", gafka.BuildId)

	startedAt := time.Now()
	closed := make(chan struct{})
	signal.RegisterHandler(func(sig os.Signal) {
		log.Info("influxwatch[%s] got signal: %s", gafka.BuildId, strings.ToUpper(sig.String()))

		if telemetry.Default != nil {
			log.Info("stopping telemetry and flush all metrics...")
			telemetry.Default.Stop()
		}

		log.Info("influxwatch[%s] shutdown complete", gafka.BuildId)
		log.Info("influxwatch[%s] %s bye!", gafka.BuildId, time.Since(startedAt))

		//notify mainline to exit
		close(closed)
	}, syscall.SIGINT, syscall.SIGTERM)

	setupTelemetry()

	postLatency := metrics.NewRegisteredHistogram("influxdb.latency.post", nil, metrics.NewExpDecaySample(1028, 0.015))
	getLatency := metrics.NewRegisteredHistogram("influxdb.latency.get", nil, metrics.NewExpDecaySample(1028, 0.015))
	postSize := metrics.NewRegisteredHistogram("influxdb.size.post", nil, metrics.NewExpDecaySample(1028, 0.015))
	getSize := metrics.NewRegisteredHistogram("influxdb.size.get", nil, metrics.NewExpDecaySample(1028, 0.015))

	reader := bufio.NewReader(os.Stdin)
	for {

		select {
		case <-closed:
			//exit
			return

		default:
			line, err := gio.ReadLine(reader)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Info("%v", err)
					continue
				}
			}

			pl, gl, ps, gs, m, err := parseLine(line)
			if err != nil {
				log.Info("%v", err)
				continue
			}

			if m == "POST" {
				postLatency.Update(pl)
				postSize.Update(ps)
			} else if m == "GET" {
				getLatency.Update(gl)
				getSize.Update(gs)
			}
		}
	}
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
func parseLine(line []byte) (postLatency, getLatency, postSize, getSize int64, method string, err error) {

	//only the line begins with [httpd] is valid
	if !httpdRegex.Match(line) {
		return 0, 0, 0, 0, "", ErrInvalidLogFormatLine
	}

	entry, err := parser.ParseString(string(line))
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	//get all fields we need
	respSizeStr, err := entry.Field("resp_bytes")
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	respSize, err := strconv.ParseInt(respSizeStr, 10, 64)
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	latencyStr, err := entry.Field("time_elapsed")
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	latency, err := strconv.ParseInt(latencyStr, 10, 64)
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	req, err := entry.Field("request")
	if err != nil {
		return 0, 0, 0, 0, "", err
	}

	method = getMethod(req)
	if method == "POST" {
		postLatency = latency //microsecond
		postSize = respSize   //bytes
	} else if method == "GET" {
		getLatency = latency //microsecond
		getSize = respSize   //bytes
	}
	return
}

func getMethod(req string) string {
	//req = r.Method+" "+uri+" "+r.Proto
	items := strings.Split(req, " ")
	return strings.ToUpper(items[0])
}
