package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

var (
	options struct {
		Id                     string
		Zone                   string
		ConfigFile             string
		PubHttpAddr            string
		PubHttpsAddr           string
		SubHttpAddr            string
		SubHttpsAddr           string
		ManHttpAddr            string
		ManHttpsAddr           string
		DebugHttpAddr          string
		Store                  string
		ManagerStore           string
		PidFile                string
		CertFile               string
		KeyFile                string
		LogFile                string
		LogLevel               string
		CrashLogFile           string
		InfluxServer           string
		KillFile               string
		ShowVersion            bool
		Ratelimit              bool
		DisableMetrics         bool
		DryRun                 bool
		ClusterAware           bool
		CpuAffinity            bool
		GolangTrace            bool
		Debug                  bool
		HttpHeaderMaxBytes     int
		MaxPubSize             int64
		MinPubSize             int
		MaxPubRetries          int
		MaxClients             int
		PubPoolCapcity         int
		PubPoolIdleTimeout     time.Duration
		SubTimeout             time.Duration
		OffsetCommitInterval   time.Duration
		ReporterInterval       time.Duration
		ConsoleMetricsInterval time.Duration
		MetaRefresh            time.Duration
		ManagerRefresh         time.Duration
		HttpReadTimeout        time.Duration
		HttpWriteTimeout       time.Duration
	}
)

func parseFlags() {
	flag.StringVar(&options.Id, "id", "", "kateway id, the id must be unique within a host")
	flag.StringVar(&options.Zone, "zone", "", "kafka zone name")
	flag.StringVar(&options.PubHttpAddr, "pubhttp", ":9191", "pub http bind addr")
	flag.StringVar(&options.PubHttpsAddr, "pubhttps", "", "pub https bind addr")
	flag.StringVar(&options.SubHttpAddr, "subhttp", ":9192", "sub http bind addr")
	flag.StringVar(&options.SubHttpsAddr, "subhttps", "", "sub https bind addr")
	flag.StringVar(&options.ManHttpAddr, "manhttp", ":9193", "management http bind addr")
	flag.StringVar(&options.ManHttpsAddr, "manhttps", "", "management https bind addr")
	flag.StringVar(&options.LogLevel, "level", "debug", "log level")
	flag.StringVar(&options.LogFile, "log", "stdout", "log file, default stdout")
	flag.StringVar(&options.CrashLogFile, "crashlog", "", "crash log")
	flag.StringVar(&options.CertFile, "certfile", "", "cert file path")
	flag.StringVar(&options.PidFile, "pid", "", "pid file")
	flag.StringVar(&options.KeyFile, "keyfile", "", "key file path")
	flag.StringVar(&options.DebugHttpAddr, "debughttp", "", "debug http bind addr")
	flag.StringVar(&options.Store, "store", "kafka", "backend store")
	flag.StringVar(&options.ManagerStore, "mstore", "mysql", "store integration with manager")
	flag.StringVar(&options.ConfigFile, "conf", "/etc/kateway.cf", "config file")
	flag.StringVar(&options.KillFile, "kill", "", "kill running kateway by pid file")
	flag.StringVar(&options.InfluxServer, "influxdb", "http://10.77.144.193:10036", "influxdb server address for the metrics reporter")
	flag.BoolVar(&options.ShowVersion, "version", false, "show version and exit")
	flag.BoolVar(&options.Debug, "debug", false, "enable debug mode")
	flag.BoolVar(&options.GolangTrace, "gotrace", false, "go tool trace")
	flag.BoolVar(&options.DryRun, "dryrun", false, "dry run mode")
	flag.BoolVar(&options.CpuAffinity, "cpuaffinity", false, "enable cpu affinity")
	flag.BoolVar(&options.Ratelimit, "raltelimit", false, "enable rate limit")
	flag.BoolVar(&options.ClusterAware, "clusteraware", false, "each kateway knows the cluster nodes")
	flag.BoolVar(&options.DisableMetrics, "metricsoff", false, "disable metrics reporter")
	flag.IntVar(&options.HttpHeaderMaxBytes, "maxheader", 4<<10, "http header max size in bytes")
	flag.Int64Var(&options.MaxPubSize, "maxpub", 1<<20, "max Pub message size")
	flag.IntVar(&options.MinPubSize, "minpub", 1, "min Pub message size")
	flag.IntVar(&options.MaxPubRetries, "pubretry", 5, "max retries when Pub fails")
	flag.IntVar(&options.PubPoolCapcity, "pubpool", 100, "pub connection pool capacity")
	flag.IntVar(&options.MaxClients, "maxclient", 100000, "max concurrent connections")
	flag.DurationVar(&options.OffsetCommitInterval, "offsetcommit", time.Minute, "consumer offset commit interval")
	flag.DurationVar(&options.HttpReadTimeout, "httprtimeout", time.Minute*5, "http server read timeout")
	flag.DurationVar(&options.HttpWriteTimeout, "httpwtimeout", time.Minute, "http server write timeout")
	flag.DurationVar(&options.SubTimeout, "subtimeout", time.Minute, "sub timeout before send http 204")
	flag.DurationVar(&options.ReporterInterval, "report", time.Second*10, "reporter flush interval")
	flag.DurationVar(&options.MetaRefresh, "metarefresh", time.Minute*10, "meta data refresh interval")
	flag.DurationVar(&options.ManagerRefresh, "manrefresh", time.Minute*5, "manager integration refresh interval")
	flag.DurationVar(&options.ConsoleMetricsInterval, "consolemetrics", 0, "console metrics report interval")
	flag.DurationVar(&options.PubPoolIdleTimeout, "pubpoolidle", 0, "pub pool connect idle timeout")

	flag.Parse()
}

func validateFlags() {
	if options.KillFile != "" {
		return
	}

	if options.Zone == "" {
		fmt.Fprintf(os.Stderr, "-zone required\n")
		os.Exit(1)
	}

	if options.ManHttpsAddr == "" && options.ManHttpAddr == "" {
		fmt.Fprintf(os.Stderr, "-manhttp or -manhttps required\n")
	}
}
