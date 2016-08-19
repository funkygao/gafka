package gateway

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
)

var (
	Options struct {
		Id                      string
		Zone                    string
		ConfigFile              string
		PubHttpAddr             string
		PubHttpsAddr            string
		SubHttpAddr             string
		SubHttpsAddr            string
		ManHttpAddr             string
		ManHttpsAddr            string
		DebugHttpAddr           string
		Store                   string
		JobStore                string
		ManagerStore            string
		PidFile                 string
		CertFile                string
		KeyFile                 string
		LogFile                 string
		LogLevel                string
		CrashLogFile            string
		DummyCluster            string
		InfluxServer            string
		InfluxDbName            string
		KillFile                string
		ShowVersion             bool
		Ratelimit               bool
		PermitStandbySub        bool
		DisableMetrics          bool
		EnableHintedHandoff     bool
		AuditPub                bool
		AuditSub                bool
		EnableGzip              bool
		DryRun                  bool
		CpuAffinity             bool
		EnableAccessLog         bool
		EnableHttpPanicRecover  bool
		GolangTrace             bool
		PermitUnregisteredGroup bool
		UseCompress             bool
		Debug                   bool
		EnableRegistry          bool
		HttpHeaderMaxBytes      int
		MaxPubSize              int64
		MaxJobSize              int64
		LogRotateSize           int
		MaxMsgTagLen            int
		MinPubSize              int
		PubQpsLimit             int64
		MaxSubBatchSize         int
		MaxClients              int
		MaxRequestPerConn       int // to make load balancer distribute request even for persistent conn
		PubPoolCapcity          int
		AssignJobShardId        int // how to assign shard id for new app
		PubPoolIdleTimeout      time.Duration
		SubTimeout              time.Duration
		OffsetCommitInterval    time.Duration
		BadClientPunishDuration time.Duration
		ReporterInterval        time.Duration
		MetaRefresh             time.Duration
		ManagerRefresh          time.Duration
		HttpReadTimeout         time.Duration
		HttpWriteTimeout        time.Duration
	}
)

func ParseFlags() {
	ip, err := ctx.LocalIP()
	if err != nil {
		panic(err)
	}

	var (
		defaultPubHttpAddr  = fmt.Sprintf("%s:9191", ip.String())
		defaultPubHttpsAddr = ""
		defaultSubHttpAddr  = fmt.Sprintf("%s:9192", ip.String())
		defaultSubHttpsAddr = ""
		defaultManHttpAddr  = fmt.Sprintf("%s:9193", ip.String())
		defaultManHttpsAddr = ""
	)

	flag.StringVar(&Options.Id, "id", "", "kateway id, the id must be unique within a host")
	flag.StringVar(&Options.Zone, "zone", "", "kafka zone name")
	flag.StringVar(&Options.PubHttpAddr, "pubhttp", defaultPubHttpAddr, "pub http bind addr")
	flag.StringVar(&Options.PubHttpsAddr, "pubhttps", defaultPubHttpsAddr, "pub https bind addr")
	flag.StringVar(&Options.SubHttpAddr, "subhttp", defaultSubHttpAddr, "sub http bind addr")
	flag.StringVar(&Options.SubHttpsAddr, "subhttps", defaultSubHttpsAddr, "sub https bind addr")
	flag.StringVar(&Options.ManHttpAddr, "manhttp", defaultManHttpAddr, "management http bind addr")
	flag.StringVar(&Options.ManHttpsAddr, "manhttps", defaultManHttpsAddr, "management https bind addr")
	flag.StringVar(&Options.LogLevel, "level", "trace", "log level")
	flag.StringVar(&Options.LogFile, "log", "stdout", "log file, default stdout")
	flag.StringVar(&Options.CrashLogFile, "crashlog", "", "crash log")
	flag.StringVar(&Options.CertFile, "certfile", "", "cert file path")
	flag.StringVar(&Options.PidFile, "pid", "", "pid file")
	flag.StringVar(&Options.KeyFile, "keyfile", "", "key file path")
	flag.StringVar(&Options.DebugHttpAddr, "debughttp", "", "debug http bind addr")
	flag.StringVar(&Options.Store, "store", "kafka", "message underlying store")
	flag.StringVar(&Options.JobStore, "jstore", "mysql", "job underlying store")
	flag.StringVar(&Options.DummyCluster, "dummycluster", "me", "dummy store's cluster name")
	flag.StringVar(&Options.ManagerStore, "mstore", "mysql", "store integration with manager")
	flag.StringVar(&Options.ConfigFile, "conf", "", "config file, defaults $HOME/.gafka.cf")
	flag.StringVar(&Options.KillFile, "kill", "", "kill running kateway by pid file")
	flag.StringVar(&Options.InfluxServer, "influxdbaddr", "", "influxdb server address for the metrics reporter")
	flag.StringVar(&Options.InfluxDbName, "influxdbname", "pubsub", "influxdb db name")
	flag.BoolVar(&Options.ShowVersion, "version", false, "show version and exit")
	flag.BoolVar(&Options.Debug, "debug", false, "enable debug mode")
	flag.BoolVar(&Options.GolangTrace, "gotrace", false, "go tool trace")
	flag.BoolVar(&Options.AuditPub, "auditpub", true, "enable Pub audit")
	flag.BoolVar(&Options.AuditSub, "auditsub", true, "enable Sub audit")
	flag.BoolVar(&Options.UseCompress, "snappy", false, "backend store will snappy compress messages")
	flag.BoolVar(&Options.EnableAccessLog, "accesslog", false, "en(dis)able access log")
	flag.BoolVar(&Options.EnableRegistry, "withreg", true, "self register in zk, otherwise isolated from cluster")
	flag.BoolVar(&Options.DryRun, "dryrun", false, "dry run mode")
	flag.BoolVar(&Options.EnableHintedHandoff, "hh", false, "enable hinted handoff for full pub availability")
	flag.BoolVar(&Options.PermitUnregisteredGroup, "unregrp", false, "permit sub group usage without being registered")
	flag.BoolVar(&Options.PermitStandbySub, "standbysub", false, "permits sub threads exceed partitions")
	flag.BoolVar(&Options.EnableGzip, "gzip", false, "enable http response gzip")
	flag.BoolVar(&Options.CpuAffinity, "cpuaffinity", false, "enable cpu affinity")
	flag.BoolVar(&Options.Ratelimit, "raltelimit", false, "enable rate limit")
	flag.BoolVar(&Options.EnableHttpPanicRecover, "httppanic", true, "enable http handler panic recover")
	flag.BoolVar(&Options.DisableMetrics, "metricsoff", false, "disable metrics reporter")
	flag.IntVar(&Options.HttpHeaderMaxBytes, "maxheader", 4<<10, "http header max size in bytes")
	flag.Int64Var(&Options.MaxPubSize, "maxpub", 512<<10, "max Pub message size")
	flag.Int64Var(&Options.MaxJobSize, "maxjob", 16<<10, "max Pub job size")
	flag.IntVar(&Options.MinPubSize, "minpub", 1, "min Pub message size")
	flag.IntVar(&Options.MaxRequestPerConn, "maxreq", -1, "max request per connection")
	flag.IntVar(&Options.AssignJobShardId, "shardid", 1, "how to assign shard id for new app")
	flag.IntVar(&Options.MaxMsgTagLen, "tagsz", 1024, "max message tag length permitted")
	// kafka Fetch maxFetchSize=1MB, so if our msg agv size is 250B, batch size can be 4000
	flag.IntVar(&Options.MaxSubBatchSize, "maxbatch", 4000, "max sub batch size")
	flag.IntVar(&Options.LogRotateSize, "logsize", 10<<30, "max unrotated log file size")
	flag.Int64Var(&Options.PubQpsLimit, "publimit", 60*10000, "pub qps limit per minute per ip")
	flag.IntVar(&Options.PubPoolCapcity, "pubpool", 100, "pub connection pool capacity")
	flag.IntVar(&Options.MaxClients, "maxclient", 100000, "max concurrent connections")
	flag.DurationVar(&Options.OffsetCommitInterval, "offsetcommit", time.Minute, "consumer offset commit interval")
	flag.DurationVar(&Options.HttpReadTimeout, "httprtimeout", time.Minute*5, "http server read timeout")
	flag.DurationVar(&Options.HttpWriteTimeout, "httpwtimeout", time.Minute, "http server write timeout")
	flag.DurationVar(&Options.SubTimeout, "subtimeout", time.Second*30, "sub timeout before send http 204")
	flag.DurationVar(&Options.ReporterInterval, "report", time.Minute, "reporter flush interval")
	flag.DurationVar(&Options.BadClientPunishDuration, "punish", time.Second*3, "punish bad client by sleep")
	flag.DurationVar(&Options.MetaRefresh, "metarefresh", time.Minute*10, "meta data refresh interval")
	flag.DurationVar(&Options.ManagerRefresh, "manrefresh", time.Minute*5, "manager integration refresh interval")
	flag.DurationVar(&Options.PubPoolIdleTimeout, "pubpoolidle", 0, "pub pool connect idle timeout")

	flag.Parse()
}

func ValidateFlags() {
	if Options.KillFile != "" {
		return
	}

	if Options.ShowVersion {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	if gafka.BuildId == "" {
		fmt.Fprintf(os.Stderr, "empty BuildId, please rebuild with build.sh\n")
		//os.Exit(1)
	}

	if Options.Zone == "" {
		fmt.Fprintf(os.Stderr, "-zone required\n")
		os.Exit(1)
	}
}
