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
	}
)

func parseFlags() {
	flag.StringVar(&options.zone, "zone", "", "kafka zone name")
	flag.StringVar(&options.cluster, "cluster", "", "kafka cluster name")
	flag.DurationVar(&options.metaRefresh, "metarefresh", time.Minute, "meta data refresh interval in seconds")

	flag.StringVar(&options.logLevel, "level", "debug", "log level")
	flag.StringVar(&options.logFile, "log", "stdout", "log file, default stdout")
	flag.StringVar(&options.crashLogFile, "crashlog", "", "crash log")
	flag.StringVar(&options.configFile, "conf", "/etc/gafka.cf", "config file")
	flag.BoolVar(&options.showVersion, "version", false, "show version and exit")
	flag.DurationVar(&options.tick, "tick", time.Second*10, "system reporter ticker")
	flag.BoolVar(&options.cpuprof, "cpuprof", false, "enable cpu profiling")
	flag.BoolVar(&options.memprof, "memprof", false, "enable memory profiling")
	flag.StringVar(&options.mode, "mode", "pub", "gateway mode: <pub|sub>")
	flag.BoolVar(&options.blockprof, "blockprof", false, "enable block profiling")

	flag.Parse()

	if options.tick < 0 {
		fmt.Fprintf(os.Stderr, "tick must be possitive\n")
		os.Exit(1)
	}
	if options.zone == "" || options.cluster == "" {
		fmt.Fprintf(os.Stderr, "zone and cluster are required\n")
		os.Exit(1)
	}
}
