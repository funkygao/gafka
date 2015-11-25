package main

import (
	"flag"
	"time"
)

var (
	options struct {
		configFile   string
		showVersion  bool
		logFile      string
		logLevel     string
		tick         time.Duration
		cpuprof      bool
		memprof      bool
		blockprof    bool
		crashLogFile string
	}
)

func parseFlags() {
	flag.StringVar(&options.logLevel, "level", "debug", "log level")
	flag.StringVar(&options.logFile, "log", "stdout", "log file, default stdout")
	flag.StringVar(&options.crashLogFile, "crashlog", "panic.dump", "crash log")
	flag.StringVar(&options.configFile, "conf", "/etc/gafka.cf", "config file")
	flag.BoolVar(&options.showVersion, "version", false, "show version and exit")
	flag.DurationVar(&options.tick, "tick", time.Minute, "system info watchdog ticker")
	flag.BoolVar(&options.cpuprof, "cpuprof", false, "enable cpu profiling")
	flag.BoolVar(&options.memprof, "memprof", false, "enable memory profiling")
	flag.BoolVar(&options.blockprof, "blockprof", false, "enable block profiling")

	flag.Parse()

	if options.tick <= 0 {
		panic("tick must be possitive")
	}
}
