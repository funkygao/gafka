package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

var (
	options struct {
		id                     string
		zone                   string
		configFile             string
		pubHttpAddr            string
		pubHttpsAddr           string
		subHttpAddr            string
		subHttpsAddr           string
		manHttpAddr            string
		manHttpsAddr           string
		store                  string
		pidFile                string
		debugHttpAddr          string
		certFile               string
		keyFile                string
		logFile                string
		logLevel               string
		crashLogFile           string
		influxServer           string
		killFile               string
		showVersion            bool
		ratelimit              bool
		disableMetrics         bool
		dryRun                 bool
		clusterAware           bool
		cpuAffinity            bool
		debug                  bool
		maxPubSize             int64
		minPubSize             int
		maxPubRetries          int
		maxClients             int
		pubPoolCapcity         int
		pubPoolIdleTimeout     time.Duration
		subTimeout             time.Duration
		offsetCommitInterval   time.Duration
		reporterInterval       time.Duration
		consoleMetricsInterval time.Duration
		metaRefresh            time.Duration
		httpReadTimeout        time.Duration
		httpWriteTimeout       time.Duration
	}
)

func parseFlags() {
	flag.StringVar(&options.id, "id", "", "kateway id, the id must be unique within a host")
	flag.StringVar(&options.zone, "zone", "", "kafka zone name")
	flag.DurationVar(&options.metaRefresh, "metarefresh", time.Minute*10, "meta data refresh interval")
	flag.StringVar(&options.pubHttpAddr, "pubhttp", ":9191", "pub http bind addr")
	flag.StringVar(&options.pubHttpsAddr, "pubhttps", "", "pub https bind addr")
	flag.StringVar(&options.subHttpAddr, "subhttp", ":9192", "sub http bind addr")
	flag.StringVar(&options.subHttpsAddr, "subhttps", "", "sub https bind addr")
	flag.StringVar(&options.manHttpAddr, "manhttp", ":9193", "management http bind addr")
	flag.StringVar(&options.manHttpsAddr, "manhttps", "", "management https bind addr")
	flag.StringVar(&options.logLevel, "level", "debug", "log level")
	flag.StringVar(&options.logFile, "log", "stdout", "log file, default stdout")
	flag.StringVar(&options.crashLogFile, "crashlog", "", "crash log")
	flag.StringVar(&options.certFile, "certfile", "", "cert file path")
	flag.StringVar(&options.pidFile, "pid", "", "pid file")
	flag.StringVar(&options.keyFile, "keyfile", "", "key file path")
	flag.StringVar(&options.debugHttpAddr, "debughttp", "", "debug http bind addr")
	flag.StringVar(&options.store, "store", "kafka", "backend store")
	flag.StringVar(&options.configFile, "conf", "/etc/kateway.cf", "config file")
	flag.BoolVar(&options.debug, "debug", false, "enable debug mode")
	flag.StringVar(&options.killFile, "kill", "", "kill running kateway by pid file")
	flag.BoolVar(&options.showVersion, "version", false, "show version and exit")
	flag.BoolVar(&options.dryRun, "dryrun", false, "dry run mode")
	flag.BoolVar(&options.cpuAffinity, "cpuaffinity", false, "enable cpu affinity")
	flag.BoolVar(&options.ratelimit, "raltelimit", false, "enable rate limit")
	flag.BoolVar(&options.clusterAware, "clusteraware", false, "each kateway knows the cluster nodes")
	flag.BoolVar(&options.disableMetrics, "metricsoff", false, "disable metrics reporter")
	flag.StringVar(&options.influxServer, "influxdb", "http://10.77.144.193:10036", "influxdb server address for the metrics reporter")
	flag.DurationVar(&options.reporterInterval, "report", time.Second*10, "reporter flush interval")
	flag.Int64Var(&options.maxPubSize, "maxpub", 1<<20, "max Pub message size")
	flag.IntVar(&options.minPubSize, "minpub", 1, "min Pub message size")
	flag.IntVar(&options.maxPubRetries, "pubretry", 5, "max retries when Pub fails")
	flag.IntVar(&options.pubPoolCapcity, "pubpool", 100, "pub connection pool capacity")
	flag.IntVar(&options.maxClients, "maxclient", 100000, "max concurrent connections")
	flag.DurationVar(&options.offsetCommitInterval, "offsetcommit", time.Minute, "consumer offset commit interval")
	flag.DurationVar(&options.httpReadTimeout, "httprtimeout", time.Second*60, "http server read timeout")
	flag.DurationVar(&options.httpWriteTimeout, "httpwtimeout", time.Second*60, "http server write timeout")
	flag.DurationVar(&options.subTimeout, "subtimeout", time.Minute, "sub timeout before send http 204")
	flag.DurationVar(&options.consoleMetricsInterval, "consolemetrics", 0, "console metrics report interval")
	flag.DurationVar(&options.pubPoolIdleTimeout, "pubpoolidle", 0, "pub pool connect idle timeout")

	flag.Parse()
}

func validateFlags() {
	if options.killFile != "" {
		return
	}

	if options.zone == "" {
		fmt.Fprintf(os.Stderr, "-zone required\n")
		os.Exit(1)
	}

	if options.manHttpsAddr == "" && options.manHttpAddr == "" {
		fmt.Fprintf(os.Stderr, "-manhttp or -manhttps required\n")
	}
}
