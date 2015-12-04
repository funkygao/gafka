package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

var (
	options struct {
		id                   string
		zone                 string
		cluster              string
		configFile           string
		pubHttpAddr          string
		pubHttpsAddr         string
		subHttpAddr          string
		subHttpsAddr         string
		store                string
		pidFile              string
		certFile             string
		keyFile              string
		logFile              string
		logLevel             string
		crashLogFile         string
		influxServer         string
		killFile             string
		cpuprof              bool
		showVersion          bool
		memprof              bool
		blockprof            bool
		debug                bool
		maxPubSize           int64
		maxClients           int
		offsetCommitInterval time.Duration
		reporterInterval     time.Duration
		metaRefresh          time.Duration
		httpReadTimeout      time.Duration
		httpWriteTimeout     time.Duration
	}
)

func parseFlags() {
	flag.StringVar(&options.id, "id", "", "kateway id, the id must be unique within a host")
	flag.StringVar(&options.zone, "zone", "", "kafka zone name")
	flag.StringVar(&options.cluster, "cluster", "", "kafka cluster name")
	flag.DurationVar(&options.metaRefresh, "metarefresh", time.Minute*10, "meta data refresh interval")
	flag.StringVar(&options.pubHttpAddr, "pubhttp", ":9191", "pub http bind addr")
	flag.StringVar(&options.pubHttpsAddr, "pubhttps", "", "pub https bind addr")
	flag.StringVar(&options.subHttpAddr, "subhttp", ":9192", "sub http bind addr")
	flag.StringVar(&options.subHttpsAddr, "subhttps", "", "sub https bind addr")
	flag.StringVar(&options.logLevel, "level", "debug", "log level")
	flag.StringVar(&options.logFile, "log", "stdout", "log file, default stdout")
	flag.StringVar(&options.crashLogFile, "crashlog", "", "crash log")
	flag.StringVar(&options.certFile, "certfile", "", "cert file path")
	flag.StringVar(&options.pidFile, "pid", "", "pid file")
	flag.StringVar(&options.keyFile, "keyfile", "", "key file path")
	flag.StringVar(&options.store, "store", "kafka", "backend store")
	flag.StringVar(&options.configFile, "conf", "/etc/gafka.cf", "config file")
	flag.BoolVar(&options.debug, "debug", false, "enable debug mode")
	flag.StringVar(&options.killFile, "kill", "", "kill running kateway by pid file")
	flag.BoolVar(&options.showVersion, "version", false, "show version and exit")
	flag.DurationVar(&options.reporterInterval, "report", time.Second*10, "reporter flush interval")
	flag.BoolVar(&options.cpuprof, "cpuprof", false, "enable cpu profiling")
	flag.BoolVar(&options.memprof, "memprof", false, "enable memory profiling")
	flag.StringVar(&options.influxServer, "influxdb", "http://10.77.144.193:10036", "influxdb server address for the metrics reporter")
	flag.BoolVar(&options.blockprof, "blockprof", false, "enable block profiling")
	flag.Int64Var(&options.maxPubSize, "maxpub", 1<<20, "max Pub message size")
	flag.IntVar(&options.maxClients, "maxclient", 10000, "max concurrent connections")
	flag.DurationVar(&options.offsetCommitInterval, "offsetcommit", time.Minute, "consumer offset commit interval")
	flag.DurationVar(&options.httpReadTimeout, "httprtimeout", time.Second*60, "http server read timeout")
	flag.DurationVar(&options.httpWriteTimeout, "httpwtimeout", time.Second*60, "http server write timeout")

	flag.Parse()
}

func validateFlags() {
	if options.killFile != "" {
		return
	}

	if options.zone == "" || options.cluster == "" {
		fmt.Fprintf(os.Stderr, "-zone and -cluster are required\n")
		os.Exit(1)
	}
}
