package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

var (
	options struct {
		zone         string
		cluster      string
		metaRefresh  time.Duration
		configFile   string
		showVersion  bool
		logFile      string
		logLevel     string
		tick         time.Duration
		mode         string
		cpuprof      bool
		memprof      bool
		blockprof    bool
		crashLogFile string
		influxServer string
		port         int
		maxBodySize  int64
	}
)

func parseFlags() {
	flag.StringVar(&options.zone, "zone", "", "kafka zone name")
	flag.StringVar(&options.cluster, "cluster", "", "kafka cluster name")
	flag.DurationVar(&options.metaRefresh, "metarefresh", time.Minute, "meta data refresh interval in seconds")
	flag.IntVar(&options.port, "port", 9090, "http bind port")
	flag.StringVar(&options.logLevel, "level", "debug", "log level")
	flag.StringVar(&options.logFile, "log", "stdout", "log file, default stdout")
	flag.StringVar(&options.crashLogFile, "crashlog", "", "crash log")
	flag.StringVar(&options.configFile, "conf", "/etc/gafka.cf", "config file")
	flag.BoolVar(&options.showVersion, "version", false, "show version and exit")
	flag.DurationVar(&options.tick, "tick", time.Second*10, "system reporter ticker")
	flag.BoolVar(&options.cpuprof, "cpuprof", false, "enable cpu profiling")
	flag.BoolVar(&options.memprof, "memprof", false, "enable memory profiling")
	flag.StringVar(&options.mode, "mode", "pub", "gateway mode: <pub|sub>")
	flag.StringVar(&options.influxServer, "influxdb", "http://10.77.144.193:10036", "influxdb server address for the metrics reporter")
	flag.BoolVar(&options.blockprof, "blockprof", false, "enable block profiling")
	flag.Int64Var(&options.maxBodySize, "maxbody", 1<<20, "max POST body size")

	flag.Parse()
}

func validateFlags() {
	if options.tick < 0 {
		fmt.Fprintf(os.Stderr, "-tick must be possitive\n")
		os.Exit(1)
	}
	if options.zone == "" || options.cluster == "" {
		fmt.Fprintf(os.Stderr, "-zone and -cluster are required\n")
		os.Exit(1)
	}
}
